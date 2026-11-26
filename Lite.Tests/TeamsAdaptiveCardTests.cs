using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9611: a Teams alert is one Adaptive Card in the message envelope a Teams webhook takes. These tests read the built
/// payload: the envelope against Microsoft's own sample, the card's version and element set, and each part the card
/// shows (severity band, title, facts, Details, incident sections, records, footer, resolution, caps).
/// </summary>
public class TeamsAdaptiveCardTests
{
    private static readonly AlertBranding Lite = EmailAlertService.Branding;
    private static readonly DateTime Now = new(2026, 11, 25, 12, 0, 0, DateTimeKind.Utc);

    /* Microsoft's sample request body for a Teams webhook, as its documentation gives it ("When a Teams webhook request
       is received" in the Microsoft Teams connector reference, and "Send Adaptive Cards using an Incoming Webhook" in the
       Teams platform documentation). */
    private const string MicrosoftSample = """
        {
           "type":"message",
           "attachments":[
              {
                 "contentType":"application/vnd.microsoft.card.adaptive",
                 "contentUrl":null,
                 "content":{
                    "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
                    "type":"AdaptiveCard",
                    "version":"1.2",
                    "body":[
                        {
                        "type": "TextBlock",
                        "text": "For Samples and Templates, see [https://adaptivecards.io/samples](https://adaptivecards.io/samples)"
                        }
                    ]
                 }
              }
           ]
        }
        """;

    private static AlertContext External() => new() { External = new AlertExternalOrigin("billing", "queue-depth", 7) };

    private static string BuiltIn(string? triageUrl = null) => WebhookAlertService.BuildTeamsPayload(
        "High CPU", "SQL01", "97%", "90%", Lite, triageUrl: triageUrl, detailText: "CPU at 97% for 5 minutes", nowUtc: Now);

    private static IEnumerable<JsonElement> Walk(JsonElement element)
    {
        yield return element;
        var children = element.ValueKind switch
        {
            JsonValueKind.Object => element.EnumerateObject().Select(p => p.Value),
            JsonValueKind.Array => element.EnumerateArray(),
            _ => [],
        };
        foreach (var child in children.SelectMany(Walk))
        {
            yield return child;
        }
    }

    /* ---------------- the envelope ---------------- */

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TheEnvelope_MatchesMicrosoftsSampleFieldByField(bool external)
    {
        var payload = external
            ? WebhookAlertService.BuildTeamsPayload("External:billing:queue-depth", "SQL01", "7", "", Lite, context: External(), detailText: "a\nb", displayName: "Report", nowUtc: Now)
            : BuiltIn("https://darling.example/triage?server=SQL01");
        var ours = JsonDocument.Parse(payload).RootElement;
        var sample = JsonDocument.Parse(MicrosoftSample).RootElement;

        /* The message: the same properties, in the same order, with the same values. */
        Assert.Equal(JsonValueKind.Object, ours.ValueKind);
        Assert.Equal(sample.EnumerateObject().Select(p => p.Name), ours.EnumerateObject().Select(p => p.Name));
        Assert.Equal(sample.GetProperty("type").GetString(), ours.GetProperty("type").GetString());

        /* One attachment, with the sample's properties and values, contentUrl a JSON null. */
        Assert.Equal(JsonValueKind.Array, ours.GetProperty("attachments").ValueKind);
        var attachment = Assert.Single(ours.GetProperty("attachments").EnumerateArray());
        var sampleAttachment = Assert.Single(sample.GetProperty("attachments").EnumerateArray());
        Assert.Equal(sampleAttachment.EnumerateObject().Select(p => p.Name), attachment.EnumerateObject().Select(p => p.Name));
        Assert.Equal(sampleAttachment.GetProperty("contentType").GetString(), attachment.GetProperty("contentType").GetString());
        Assert.Equal(JsonValueKind.Null, sampleAttachment.GetProperty("contentUrl").ValueKind);
        Assert.Equal(JsonValueKind.Null, attachment.GetProperty("contentUrl").ValueKind);

        /* The card: an object (not a JSON string), with the sample's $schema, type and version, and a body array. */
        var content = attachment.GetProperty("content");
        var sampleContent = sampleAttachment.GetProperty("content");
        Assert.Equal(JsonValueKind.Object, content.ValueKind);
        foreach (var name in new[] { "$schema", "type", "version" })
        {
            Assert.Equal(sampleContent.GetProperty(name).GetString(), content.GetProperty(name).GetString());
        }

        Assert.Equal(JsonValueKind.String, content.GetProperty("version").ValueKind);
        Assert.Equal(WebhookAlertService.TeamsCardVersion, content.GetProperty("version").GetString());
        Assert.Equal(JsonValueKind.Array, content.GetProperty("body").ValueKind);
        Assert.NotEmpty(content.GetProperty("body").EnumerateArray());

        /* Beyond the sample's four card properties the card has only the full-width request and, with a link, its actions. */
        var expected = new List<string> { "$schema", "type", "version", "msteams", "body" };
        if (!external)
        {
            expected.Add("actions");
        }

        Assert.Equal(expected, content.EnumerateObject().Select(p => p.Name));
        Assert.Equal("""{"width":"Full"}""", content.GetProperty("msteams").GetRawText());
    }

    [Fact]
    public void TheEnvelope_HasNothingOfAMessageCard()
    {
        var payload = BuiltIn("https://darling.example/triage?server=SQL01");

        foreach (var name in new[] { "MessageCard", "@type", "@context", "themeColor", "\"summary\"", "\"sections\"", "potentialAction", "OpenUri", "activityTitle" })
        {
            Assert.DoesNotContain(name, payload, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheCard_UsesOnlyElementsOfItsDeclaredVersion_AndSendsNoNullButContentUrl()
    {
        var context = External();
        context.Details.Add(new AlertDetailItem { Heading = "Fields", Fields = { ("Label", "value") } });
        var records = new AlertDetailItem { Heading = "Top Queries" };
        records.Records.Add(new AlertDetailRecord(1, "one", [("Query Text", "SELECT 1")]));
        context.Details.Add(records);
        var payload = WebhookAlertService.BuildTeamsPayload(
            "External:billing:queue-depth", "SQL01", "7", "", Lite, context: context,
            triageUrl: "https://darling.example/triage?server=SQL01", detailText: "a\nb", displayName: "Report", nowUtc: Now);
        var root = JsonDocument.Parse(payload).RootElement;

        var types = Walk(root.GetProperty("attachments")[0].GetProperty("content"))
            .Where(e => e.ValueKind == JsonValueKind.Object && e.TryGetProperty("type", out _))
            .Select(e => e.GetProperty("type").GetString()!)
            .Distinct().Order(StringComparer.Ordinal).ToList();
        /* Every one is in Adaptive Cards 1.2 or earlier, and Action.OpenUrl is an action a webhook card may carry. */
        Assert.Equal(["Action.OpenUrl", "AdaptiveCard", "Container", "FactSet", "RichTextBlock", "TextBlock", "TextRun"], types);

        Assert.Equal(1, Walk(root).Count(e => e.ValueKind == JsonValueKind.Null));
        Assert.All(
            Walk(root).Where(e => e.ValueKind == JsonValueKind.Object && e.TryGetProperty("type", out var t) && t.GetString() == "TextBlock"),
            block => Assert.True(block.GetProperty("wrap").GetBoolean()));
    }

    /* ---------------- severity color ---------------- */

    [Theory]
    [InlineData("#DC2626", "attention")]
    [InlineData("#D97706", "warning")]
    [InlineData("#F59E0B", "warning")]
    [InlineData("#16A34A", "good")]
    [InlineData("#2eaef1", "accent")]
    [InlineData("not a color", "accent")]
    public void ASeverityColor_BecomesTheNearestContainerStyle(string hex, string style) =>
        Assert.Equal(style, WebhookAlertService.TeamsCardStyle(hex));

    [Theory]
    [InlineData("Volume Free Space", AlertSeverityLevel.Critical, "attention", "CRITICAL")]
    [InlineData("Volume Free Space", AlertSeverityLevel.Warning, "warning", "WARNING")]
    [InlineData("Server Restored", null, "good", "RESOLVED")]
    [InlineData("A Metric With No Severity Of Its Own", null, "accent", "INFO")]
    public void TheHeader_IsABandInTheSeveritysStyle_HoldingTheTitleAndTheEditionAndServerLine(
        string metric, AlertSeverityLevel? level, string style, string badge)
    {
        var payload = WebhookAlertService.BuildTeamsPayload(
            metric, "SQL01", "1", "2", Lite, context: new AlertContext { SeverityOverride = level }, nowUtc: Now);
        var card = TeamsCard.Parse(payload);

        Assert.Equal(style, card.Style);
        Assert.True(card.Bleed);
        Assert.EndsWith($" {badge} — {metric}", card.Title, StringComparison.Ordinal);
        Assert.Equal("Performance Monitor Lite — SQL01", card.Subtitle);

        var header = card.Content.GetProperty("body")[0];
        Assert.Equal("Container", header.GetProperty("type").GetString());
        var title = header.GetProperty("items")[0];
        Assert.Equal(("TextBlock", "Bolder", "Large", true),
            (title.GetProperty("type").GetString(), title.GetProperty("weight").GetString(), title.GetProperty("size").GetString(), title.GetProperty("wrap").GetBoolean()));
        var subtitle = header.GetProperty("items")[1];
        Assert.Equal(("TextBlock", true, "None"),
            (subtitle.GetProperty("type").GetString(), subtitle.GetProperty("isSubtle").GetBoolean(), subtitle.GetProperty("spacing").GetString()));
    }

    /* ---------------- a built-in alert, on Lite ---------------- */

    [Fact]
    public void ABuiltInAlertOnLite_ShowsItsFactsItsDetailsAndTheSnoozeFooter_AndHasNoButton()
    {
        var card = TeamsCard.Parse(BuiltIn());

        Assert.EndsWith(" — High CPU", card.Title, StringComparison.Ordinal);
        Assert.Equal(["Server", "Current Value", "Threshold", "Time (UTC)", "Time (Local)"], card.Facts.Select(f => f.Title));
        Assert.Equal(("Server", "SQL01"), card.Facts[0]);
        Assert.Equal(("Current Value", "97%"), card.Facts[1]);
        Assert.Equal(("Threshold", "90%"), card.Facts[2]);
        Assert.Equal(("Time (UTC)", "2026-11-25 12:00:00"), card.Facts[3]);

        Assert.Equal(2, card.Sections.Count);
        Assert.Equal(["CPU at 97% for 5 minutes"], card.Section("Details").Texts);
        Assert.Null(card.Sections[1].Heading);
        Assert.Equal([Lite.SnoozeHint!], card.Sections[1].Texts);

        Assert.Null(card.Action);
        Assert.False(card.Content.TryGetProperty("actions", out _));
    }

    [Fact]
    public void ATestSend_ShowsTheTestTitleAndStatus_WithNoFooterAndNoButton()
    {
        var card = TeamsCard.Parse(WebhookAlertService.BuildTeamsPayload(
            "Test Notification", "", "Webhook configuration verified", "", Lite, isTest: true, nowUtc: Now));

        Assert.EndsWith(" TEST — Test Notification", card.Title, StringComparison.Ordinal);
        Assert.Equal("Performance Monitor Lite", card.Subtitle);
        Assert.Equal(["Status", "Sent at"], card.Facts.Select(f => f.Title));
        Assert.Equal("Webhook configuration is working correctly", card.Facts[0].Value);
        Assert.Empty(card.Sections);
        Assert.Null(card.Action);
    }

    /* ---------------- an external alert ---------------- */

    [Fact]
    public void AnExternalAlert_KeepsItsMarkInTheTitle_AndItsDetailLinesAsParagraphs()
    {
        var card = TeamsCard.Parse(WebhookAlertService.BuildTeamsPayload(
            "External:billing:queue-depth", "SQL01", "7", "", Lite,
            context: External(), detailText: "Nightly report\nrows *failed*: 3 <b>&\n\nNotes:\n  indented   gap", displayName: "Nightly report", nowUtc: Now));

        Assert.EndsWith(@" — \[External: billing\] Nightly report", card.Title, StringComparison.Ordinal);
        /* Each line feed is a paragraph break, the text is escaped, a line is sent without its leading spaces, and the spaces
           inside a line are sent as the source wrote them. */
        Assert.Equal(
            ["Nightly report\n\nrows \\*failed\\*: 3 &lt;b&gt;&amp;\n\n\n\nNotes:\n\nindented   gap"],
            card.Section("Details").Texts);
        Assert.DoesNotContain(card.Facts, f => f.Title == "Threshold");
    }

    [Fact]
    public void AnExternalDetailLineThatOpensWithAMarker_KeepsTheMarkerAsTextInTheDetailsBlock()
    {
        var card = TeamsCard.Parse(WebhookAlertService.BuildTeamsPayload(
            "External:billing:queue-depth", "SQL01", "7", "", Lite,
            context: External(), detailText: "Notes\n- retry queue is 14\n  + 2 added\n# of failed rows: 3\n1. rerun the job", displayName: "Report", nowUtc: Now));

        /* #9607: a TextBlock reads a line that opens with "- ", "+ ", "# " or "1. " as a list item or a heading, so the marker
           is escaped, after the line's indentation is dropped, and the marker shows as the source wrote it. */
        Assert.Equal(
            ["Notes\n\n\\- retry queue is 14\n\n\\+ 2 added\n\n\\# of failed rows: 3\n\n1\\. rerun the job"],
            card.Section("Details").Texts);
    }

    [Fact]
    public void TheDetailLineForm_TheCardSends_IsTheDoubledLineFeedInOneTextBlock()
    {
        var blocks = WebhookAlertService.TeamsDetailBlocks("Report\n- 3 failed", external: true, WebhookAlertService.TeamsExternalDetailLines);

        using var document = JsonDocument.Parse(JsonSerializer.Serialize(blocks));
        Assert.Equal("Report\n\n\\- 3 failed", Assert.Single(document.RootElement.EnumerateArray().ToList()).GetProperty("text").GetString());
    }

    [Fact]
    public void TheDetailLineForm_BlockPerLine_SendsOneWrappedTextBlockPerLine_WithTheMarkerEscapeKept()
    {
        var blocks = WebhookAlertService.TeamsDetailBlocks(
            "Report\n  rows: 120\n\n- 3 failed", external: true, WebhookAlertService.TeamsDetailLines.BlockPerLine);

        using var document = JsonDocument.Parse(JsonSerializer.Serialize(blocks));
        var sent = document.RootElement.EnumerateArray().ToList();
        Assert.Equal(["Report", "rows: 120", "\\- 3 failed"], sent.Select(block => block.GetProperty("text").GetString()));
        Assert.All(sent, block =>
        {
            Assert.Equal("TextBlock", block.GetProperty("type").GetString());
            Assert.True(block.GetProperty("wrap").GetBoolean());
        });
    }

    [Fact]
    public void AnExternalDetailLineIndentedFourOrMoreColumns_IsSentWithoutItsIndent_InEitherForm()
    {
        /* A markdown reader that follows CommonMark reads a line indented four or more columns as a code block, both after a
           blank line (the doubled line feed) and as the first line of a TextBlock (one block per line). */
        foreach (var form in new[] { WebhookAlertService.TeamsDetailLines.DoubledLineFeed, WebhookAlertService.TeamsDetailLines.BlockPerLine })
        {
            var blocks = WebhookAlertService.TeamsDetailBlocks(
                "Report\n    column  one\n        - deeper\n\t\ttabbed", external: true, form);

            using var document = JsonDocument.Parse(JsonSerializer.Serialize(blocks));
            var lines = document.RootElement.EnumerateArray()
                .SelectMany(block => block.GetProperty("text").GetString()!.Split('\n'))
                .Where(line => line.Length > 0)
                .ToList();
            Assert.Equal(["Report", "column  one", "\\- deeper", "tabbed"], lines);
        }
    }

    [Fact]
    public void TheDetailLineForm_LeavesAnEngineAlertsProseAsOneBlockInEitherForm()
    {
        foreach (var form in new[] { WebhookAlertService.TeamsDetailLines.DoubledLineFeed, WebhookAlertService.TeamsDetailLines.BlockPerLine })
        {
            var blocks = WebhookAlertService.TeamsDetailBlocks("first\n- second", external: false, form);

            using var document = JsonDocument.Parse(JsonSerializer.Serialize(blocks));
            Assert.Equal("first\n- second", Assert.Single(document.RootElement.EnumerateArray().ToList()).GetProperty("text").GetString());
        }
    }

    /* ---------------- incident sections and records ---------------- */

    [Fact]
    public void ARecordSection_KeepsRowsWithNoTextInOneFactSet_AndPutsEachTextUnderItsOwnRow()
    {
        var context = new AlertContext();
        var item = new AlertDetailItem { Heading = "Top [Queries]" };
        item.Records.Add(new AlertDetailRecord(1, "first *row*", []));
        item.Records.Add(new AlertDetailRecord(2, "second", [("Query Text", "SELECT * FROM [dbo].[t_1]\nWHERE a_b = `x`")]));
        item.Records.Add(new AlertDetailRecord(3, "third", [("Blocked", "UPDATE t"), ("Blocking", "SELECT 1")]));
        item.Records.Add(new AlertDetailRecord(4, "fourth", []));
        context.Details.Add(item);

        var payload = WebhookAlertService.BuildTeamsPayload("Blocking Detected", "SQL01", "4", "1", Lite, context: context, nowUtc: Now);
        var card = TeamsCard.Parse(payload);
        var section = card.Section(@"Top \[Queries\]");

        Assert.Equal([("#1", @"first \*row\*"), ("#2", "second"), ("#3", "third"), ("#4", "fourth")], section.Facts);
        /* A TextRun shows its text as it is: nothing is escaped, and a line break is one space. */
        Assert.Equal(["SELECT * FROM [dbo].[t_1] WHERE a_b = `x`", "UPDATE t", "SELECT 1"], section.Runs);
        Assert.Equal(["_Blocked_", "_Blocking_"], section.Texts);

        var container = card.Content.GetProperty("body").EnumerateArray().Single(e =>
            e.TryGetProperty("items", out var items) && items[0].GetProperty("text").GetString() == @"Top \[Queries\]");
        Assert.Equal(
            ["TextBlock", "FactSet", "RichTextBlock", "FactSet", "TextBlock", "RichTextBlock", "TextBlock", "RichTextBlock", "FactSet"],
            container.GetProperty("items").EnumerateArray().Select(e => e.GetProperty("type").GetString()));
        Assert.Equal([2, 1, 1], container.GetProperty("items").EnumerateArray()
            .Where(e => e.GetProperty("type").GetString() == "FactSet").Select(e => e.GetProperty("facts").GetArrayLength()));
    }

    [Fact]
    public void AnAdviceItemAndARemediationItem_StayInTheLeadFacts_AndAFieldsItemWithNoFieldsIsAHeadingAlone()
    {
        var context = new AlertContext();
        context.Details.Add(new AlertDetailItem { Heading = "Check the graph", Body = "Investigation: look [here].\n\nRemediation: fix it." });
        context.Details.Add(new AlertDetailItem { Heading = "Fix script", IsCodeBlock = true, Body = "KILL 55;" });
        context.Details.Add(new AlertDetailItem { Heading = "Empty" });

        var payload = WebhookAlertService.BuildTeamsPayload("Deadlocks Detected", "SQL01", "2", "1", Lite, context: context, nowUtc: Now);
        var card = TeamsCard.Parse(payload);

        Assert.Contains(("Advice", "Check the graph"), card.Facts);
        Assert.Contains(("Investigation", @"look \[here\]."), card.Facts);
        Assert.Contains(("Remediation", "fix it."), card.Facts);
        Assert.Contains(card.Facts, f => f.Title == "Fix script" && f.Value.Contains("email", StringComparison.Ordinal));
        Assert.DoesNotContain("KILL 55", payload, StringComparison.Ordinal);

        var empty = card.Section("Empty");
        Assert.Empty(empty.Facts);
        Assert.DoesNotContain("\"facts\":[]", payload, StringComparison.Ordinal);
    }

    /* ---------------- a resolution ---------------- */

    [Fact]
    public void AResolution_IsGreen_HasNoThresholdFact_AndKeepsItsLines()
    {
        var card = TeamsCard.Parse(WebhookAlertService.BuildTeamsPayload(
            "High CPU Resolved", "SQL01", "41%", "", Lite,
            detailText: "Alert: High CPU\n\nFired: 2026-11-25 11:00:00 UTC\n\nCleared: 2026-11-25 12:00:00 UTC", nowUtc: Now));

        Assert.Equal("good", card.Style);
        Assert.EndsWith(" RESOLVED — High CPU Resolved", card.Title, StringComparison.Ordinal);
        Assert.Equal(["Server", "Current Value", "Time (UTC)", "Time (Local)"], card.Facts.Select(f => f.Title));
        Assert.Equal(
            ["Alert: High CPU\n\nFired: 2026-11-25 11:00:00 UTC\n\nCleared: 2026-11-25 12:00:00 UTC"],
            card.Section("Details").Texts);
    }

    /* ---------------- caps ---------------- */

    [Fact]
    public void AHugeDetail_KeepsTheWholePayloadUnderTheTeamsCeiling_AndSaysItWasCut()
    {
        var names = string.Join(", ", Enumerable.Range(0, 3000).Select(i => $"Tenant_{i:D4} (12 days old)"));
        var payload = WebhookAlertService.BuildTeamsPayload(
            "Log Backup Overdue", "SQL01", "3000 overdue", "older than 2 hours", Lite,
            triageUrl: "https://darling.example/triage?server=SQL01", detailText: "Overdue on: " + names + ".", nowUtc: Now);

        Assert.True(Encoding.UTF8.GetByteCount(payload) < WebhookAlertService.TeamsPayloadMaxBytes);
        var text = Assert.Single(TeamsCard.Parse(payload).Section("Details").Texts);
        Assert.EndsWith("_The rest of this detail is cut to fit the Teams size limit._", text, StringComparison.Ordinal);
        Assert.True(JsonEncodedText.Encode(text).EncodedUtf8Bytes.Length <= WebhookAlertService.TeamsProseMaxBytes);
    }

    [Fact]
    public void AnExternalDetailAtItsFiftyLineAndTwoThousandCharacterCaps_StaysFarUnderTheTeamsCeiling()
    {
        var detail = string.Join("\n", Enumerable.Range(0, 50).Select(i => $"# {i:D2} <&> " + new string('é', 30)));
        Assert.True(detail.Length <= 2000);

        var payload = WebhookAlertService.BuildTeamsPayload(
            "External:billing:queue-depth", "SQL01", "7", "", Lite, context: External(), detailText: detail, displayName: "Report", nowUtc: Now);

        var bytes = Encoding.UTF8.GetByteCount(payload);
        Assert.True(bytes < WebhookAlertService.TeamsPayloadMaxBytes, $"{bytes} bytes");
        var text = Assert.Single(TeamsCard.Parse(payload).Section("Details").Texts);
        Assert.Equal(50, text.Split("\n\n").Length);
        Assert.DoesNotContain("cut to fit", text, StringComparison.Ordinal);
    }
}
