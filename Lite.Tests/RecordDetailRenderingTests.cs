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
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3644: the High CPU card's "Top Cpu Queries" was unreadable on Slack. The producer flattened every
/// drill-down row into one field per attribute (<c>#1 Database</c>, <c>#1 Query Hash</c>, … <c>#1 Query
/// Text</c>, <c>#2 Database</c>, …) and Slack lays a section's <c>fields</c> out two across in submission
/// order, so seven attributes per query meant no query's attributes ever stayed together: #1's text beside
/// #2's hash, #3's database beside #2's SQL, a multi-line text inflating its grid row so the label/value
/// adjacency below it broke. Read live on a production page during a sustained CPU arc.
///
/// <para><b>The fix is in the producer's shape and in three renderers.</b> <c>FlattenInto</c> now ALSO packs
/// each row of an array of objects as an <see cref="AlertDetailRecord"/> — its ordinal, one summary line of
/// its non-empty scalars in the row's own order, and its SQL text(s) separately — beside the flat fields it
/// has always produced. Slack renders records as one bold summary line over a code-blocked text per row,
/// packed into as few sections as fit the text ceiling; email and Teams render the same compact list. Every
/// surface that lays fields out in one column (the persisted row, the in-app grid, PagerDuty, the generic
/// webhook, the redundancy oracle) keeps the fields and is untouched.</para>
///
/// <para><b>The two things a change like this can break.</b> It can repaint a payload that was fine — every
/// engine alert, every hand-built detail — so the byte-identity arms build a context with every non-record
/// shape (fields, body, code block, plain object, scalar array) and assert each surface's output equals the
/// pre-#3644 rendering, restated here. And a renderer with a ceiling can cut silently, so the cut arm puts
/// a text past the record cap and asserts the note states its count in characters and lands on a whole
/// character (#3622), inside every Slack ceiling.</para>
/// </summary>
public class RecordDetailRenderingTests
{
    private static AlertBranding Branding => EmailAlertService.Branding;

    private static readonly DateTime s_now = new(2026, 9, 18, 15, 42, 0, DateTimeKind.Utc);

    private const int TextObjectLimit = 3000;
    private const string Pointer = "see email or in-app Alert Details for the full text";

    /* ---------------- fixtures ---------------- */

    private static string QueryText(int i) => string.Create(CultureInfo.InvariantCulture,
        $"SELECT o.OrderId, o.OrderDate, ol.Quantity FROM Sales.Orders AS o JOIN Sales.OrderLines AS ol ON ol.OrderId = o.OrderId WHERE o.OrderDate >= @Start AND c.Region = @Region{i} OPTION (RECOMPILE)");

    /// <summary>A CPU_SPIKE finding carrying exactly the Top Cpu Queries drill-down the Lite collector
    /// emits — the shape and property order of <c>CollectTopCpuQueries</c>, five rows as it writes them.</summary>
    private static AnalysisFinding HighCpuFinding(int rows = 5, Func<int, object>? row = null)
    {
        var topCpu = new List<object>();
        for (var i = 0; i < rows; i++)
        {
            topCpu.Add(row?.Invoke(i) ?? new
            {
                database = "ReportingDB",
                query_hash = string.Create(CultureInfo.InvariantCulture, $"0x9A3F5C21D4E8B7A{i}"),
                total_cpu_ms = 3_088_689.0 - i * 100_000,
                execution_count = 159_665L - i * 1_000,
                max_dop = 16,
                spills = 0L + i,
                query_text = QueryText(i)
            });
        }

        return new AnalysisFinding
        {
            ServerId = 3,
            ServerName = "SQLPROD-A",
            Category = "cpu",
            StoryPath = "CPU_SPIKE",
            StoryPathHash = "cpu-0001",
            Severity = 0.9,
            Confidence = 0.8,
            FactCount = 1,
            RootFactKey = "CPU_SPIKE",
            RootFactValue = 96.4,
            TimeRangeStart = s_now.AddHours(-4),
            TimeRangeEnd = s_now,
            DrillDown = new Dictionary<string, object> { ["top_cpu_queries"] = topCpu }
        };
    }

    private static AlertDetailItem TopCpuItem(AnalysisFinding finding) =>
        Assert.Single(FindingMessageFormatter.BuildContext(finding, 1.5).Details, d => d.Heading == "Top Cpu Queries");

    /// <summary>Every detail shape a producer emits that is NOT a record array: fields, advice body, a code
    /// block, a plain object, a scalar array, and an incident-style item. None carries records.</summary>
    private static AlertContext NonRecordContext()
    {
        var context = new AlertContext();
        var diagnosis = new AlertDetailItem { Heading = "Diagnosis" };
        diagnosis.Fields.Add(("Story", "BLOCKING → LONG_RUNNING"));
        diagnosis.Fields.Add(("Severity", "1.20"));
        diagnosis.Fields.Add(("Database", "SalesDB"));
        context.Details.Add(diagnosis);
        context.Details.Add(new AlertDetailItem { Heading = "Find the head blocker", Body = "Investigation: look at the head.\n\nRemediation: fix the index." });
        context.Details.Add(new AlertDetailItem { Heading = "Remediation T-SQL", Body = "SELECT 1;", IsCodeBlock = true });
        var chain = new AlertDetailItem { Heading = "Blocking Chain 1" };
        chain.Fields.Add(("Database", "SalesDB"));
        chain.Fields.Add(("Blocked SQL", "UPDATE dbo.Orders SET x = 1 WHERE id = 5"));
        chain.Fields.Add(("Blocking SQL", "SELECT * FROM dbo.Orders WITH (HOLDLOCK)"));
        chain.Fields.Add(("Wait Time Ms", "12400"));
        context.Details.Add(chain);
        var plain = new AlertDetailItem { Heading = "Spike Peak" };
        plain.Fields.Add(("Time", "2026-09-18T11:40:00Z"));
        plain.Fields.Add(("Cpu Percent", "96.4"));
        context.Details.Add(plain);
        var scalars = new AlertDetailItem { Heading = "Objects" };
        scalars.Fields.Add(("#1", "dbo.Orders"));
        scalars.Fields.Add(("#2", "dbo.OrderLines"));
        context.Details.Add(scalars);
        Assert.All(context.Details, d => Assert.Empty(d.Records));
        return context;
    }

    private static List<JsonElement> SlackBlocks(string payload) =>
        JsonDocument.Parse(payload).RootElement.GetProperty("attachments")[0].GetProperty("blocks").EnumerateArray().ToList();

    private static string? SectionText(JsonElement block) =>
        block.GetProperty("type").GetString() == "section" && block.TryGetProperty("text", out var t)
            ? t.GetProperty("text").GetString()
            : null;

    private static string Slack(AlertContext context) =>
        WebhookAlertService.BuildSlackPayload("Analysis finding", "SQLPROD-A", "0.90", "1.5", Branding,
            context: context, triageUrl: "https://example.invalid/triage/1", nowUtc: s_now);

    private static string Teams(AlertContext context) =>
        WebhookAlertService.BuildTeamsPayload("Analysis finding", "SQLPROD-A", "0.90", "1.5", Branding,
            context: context, triageUrl: "https://example.invalid/triage/1", nowUtc: s_now);

    private static (string Html, string Text) Email(AlertContext context) =>
        EmailTemplateBuilder.BuildAlertEmail("Analysis finding", "SQLPROD-A", "0.90", "1.5", 30, Branding, context);

    /* ---------------- the producer ---------------- */

    /// <summary>
    /// An array of objects flattens to BOTH shapes: the flat <c>#N Label</c> fields exactly as before —
    /// seven per row, three rows, the 300-character cut on the text — and one record per kept row. Rows
    /// past the third are not carried in either shape.
    /// </summary>
    [Fact]
    public void AnArrayOfObjects_CarriesTheFlatFieldsUnchanged_AndOneRecordPerRow()
    {
        var item = TopCpuItem(HighCpuFinding());

        Assert.Equal(21, item.Fields.Count);
        Assert.Equal(new[] { "#1 Database", "#1 Query Hash", "#1 Total Cpu Ms", "#1 Execution Count", "#1 Max Dop", "#1 Spills", "#1 Query Text" },
            item.Fields.Take(7).Select(f => f.Label));
        Assert.Equal("ReportingDB", item.Fields[0].Value);
        Assert.Equal("3088689", item.Fields[2].Value);
        Assert.Equal(QueryText(0), item.Fields[6].Value);
        Assert.StartsWith("#3 ", item.Fields[^1].Label, StringComparison.Ordinal);

        Assert.Equal(3, item.Records.Count);
        Assert.Equal(new[] { 1, 2, 3 }, item.Records.Select(r => r.Ordinal));
    }

    /// <summary>
    /// The summary line is the row's scalars in the ROW's property order — identity first, measures after,
    /// as the collector wrote them — as <c>Label: value</c> joined by <c>·</c>, numbers group-separated,
    /// without the ordinal (each renderer sets it) and without the text. The text is the property whose
    /// name says it is SQL, labelled as its flat field is, and carried WHOLE: the flat field's 300-character
    /// cut is not applied, because the renderer with a ceiling states its own.
    /// </summary>
    [Fact]
    public void TheSummaryIsTheScalarsInRowOrder_AndTheTextIsTheSqlProperty()
    {
        var item = TopCpuItem(HighCpuFinding());
        var first = item.Records[0];

        Assert.Equal("Database: ReportingDB · Query Hash: 0x9A3F5C21D4E8B7A0 · Total Cpu Ms: 3,088,689 · Execution Count: 159,665 · Max Dop: 16 · Spills: 0", first.Summary);
        var (label, text) = Assert.Single(first.Texts);
        Assert.Equal("Query Text", label);
        Assert.Equal(QueryText(0), text);
        Assert.DoesNotContain("Query Text", first.Summary, StringComparison.Ordinal);
    }

    /// <summary>A long text is carried whole in the record and cut at 300 in the flat field — the two
    /// projections of one row differ only there.</summary>
    [Fact]
    public void ALongText_IsWholeInTheRecord_AndCutInTheFlatField()
    {
        var text = new string('x', 500);
        var item = TopCpuItem(HighCpuFinding(1, _ => new { database = "D", query_hash = "0x1", query_text = text }));

        Assert.Equal(text, Assert.Single(item.Records[0].Texts).Text);
        var field = Assert.Single(item.Fields, f => f.Label == "#1 Query Text");
        Assert.Equal(301, field.Value.Length);
        Assert.Equal(text[..300] + "…", field.Value);
    }

    /// <summary>Nulls and empty strings are left out of the summary (the non-AG <c>replica_role</c> is empty
    /// on nearly every server) but kept in the flat fields; booleans and fractions render as written;
    /// a nested value renders as its compact JSON; several SQL properties become several labelled texts in
    /// property order; an empty text is not carried.</summary>
    [Fact]
    public void TheSummarySkipsEmptyScalars_AndARowCanCarrySeveralTexts()
    {
        var item = TopCpuItem(HighCpuFinding(1, _ => new
        {
            database = "SalesDB",
            replica_role = "",
            regression_factor = 14.25,
            worker_ratio = 221.376,
            cofired = true,
            plan = new { id = 7 },
            nothing = (string?)null,
            blocked_sql = "UPDATE dbo.Orders SET x = 1",
            blocking_sql = "SELECT * FROM dbo.Orders WITH (HOLDLOCK)",
            victim_sql = ""
        }));

        var record = item.Records[0];
        Assert.Equal("Database: SalesDB · Regression Factor: 14.25 · Worker Ratio: 221.38 · Cofired: true · Plan: {\"id\":7}", record.Summary);
        Assert.Equal(new[] { ("Blocked Sql", "UPDATE dbo.Orders SET x = 1"), ("Blocking Sql", "SELECT * FROM dbo.Orders WITH (HOLDLOCK)") }, record.Texts);

        Assert.Contains(item.Fields, f => f.Label == "#1 Replica Role" && f.Value == "");
        Assert.Contains(item.Fields, f => f.Label == "#1 Victim Sql" && f.Value == "");
    }

    /// <summary>An array of SCALARS and a plain OBJECT keep the flat shape alone — they are genuinely paired
    /// scalars, which is what fields are for — and an array that mixes objects and scalars does too.</summary>
    [Fact]
    public void ScalarArrays_PlainObjects_AndMixedArrays_CarryNoRecords()
    {
        var finding = HighCpuFinding();
        finding.DrillDown!["objects"] = new List<object> { "dbo.Orders", "dbo.OrderLines" };
        finding.DrillDown!["spike_peak"] = new { time = "2026-09-18T11:40:00Z", cpu_percent = 96.4 };
        finding.DrillDown!["mixed"] = new List<object> { new { a = 1 }, "loose" };
        var context = FindingMessageFormatter.BuildContext(finding, 1.5);

        var objects = Assert.Single(context.Details, d => d.Heading == "Objects");
        Assert.Empty(objects.Records);
        Assert.Equal(new[] { ("#1", "dbo.Orders"), ("#2", "dbo.OrderLines") }, objects.Fields);

        var peak = Assert.Single(context.Details, d => d.Heading == "Spike Peak");
        Assert.Empty(peak.Records);
        Assert.Equal(2, peak.Fields.Count);

        var mixed = Assert.Single(context.Details, d => d.Heading == "Mixed");
        Assert.Empty(mixed.Records);
        Assert.Equal(new[] { ("#1 A", "1"), ("#2", "loose") }, mixed.Fields);
    }

    /* ---------------- Slack ---------------- */

    /// <summary>
    /// The shape the reader sees: the divider, then ONE section — <c>*Top Cpu Queries*</c>, and per row a
    /// bold <c>*#N · summary*</c> line over the SQL in a triple-backtick block — in row order, top to bottom.
    /// No <c>fields</c> array anywhere in the detail: the grid that interleaved is gone. Two blocks where
    /// the field grid cost four (heading + 21 fields = 22, in sections of ten).
    /// </summary>
    [Fact]
    public void Slack_RendersARecordDetailAsOneSectionOfStackedRecords_AndNoFieldGrid()
    {
        var finding = HighCpuFinding();
        var context = FindingMessageFormatter.BuildContext(finding, 1.5);
        var item = TopCpuItem(finding);
        var blocks = SlackBlocks(Slack(context));

        var at = blocks.FindIndex(b => SectionText(b)?.StartsWith("*Top Cpu Queries*", StringComparison.Ordinal) == true);
        Assert.True(at > 0);
        Assert.Equal("divider", blocks[at - 1].GetProperty("type").GetString());
        Assert.False(blocks[at].TryGetProperty("fields", out _));
        /* The detail's run ends where the next detail (or the footer) starts: exactly divider + one section. */
        var next = blocks[at + 1].GetProperty("type").GetString();
        Assert.True(next is "divider" or "actions" or "context", $"a second block of type {next} followed the record section");

        var text = SectionText(blocks[at])!;
        Assert.True(text.Length <= TextObjectLimit);
        var expected = new StringBuilder("*Top Cpu Queries*");
        foreach (var record in item.Records)
        {
            expected.Append('\n').Append(string.Create(CultureInfo.InvariantCulture, $"*#{record.Ordinal} · {record.Summary}*"))
                .Append("\n```\n").Append(record.Texts[0].Text).Append("\n```");
        }

        Assert.Equal(expected.ToString(), text);

        /* And the reading order is vertical: #1's text comes before #2's summary. */
        var text1 = text.IndexOf(QueryText(0), StringComparison.Ordinal);
        var summary2 = text.IndexOf("*#2 · ", StringComparison.Ordinal);
        Assert.True(text1 > 0 && summary2 > text1);
        Assert.DoesNotContain("*#1 Database:*", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// Records pack: one section while they fit the text ceiling, and a record that would cross it opens the
    /// next section with no repeated heading. Three records with 900-character texts fit one section (under
    /// 3,000 with the heading); three with 1,400-character texts do not — the third opens a second section
    /// — and no record is ever split across two.
    /// </summary>
    [Theory]
    [InlineData(900, 1)]
    [InlineData(1400, 2)]
    public void Slack_PacksRecordsIntoAsFewSectionsAsFit_AndNeverSplitsOne(int textLength, int expectedSections)
    {
        var finding = HighCpuFinding(3, i => new { database = "D", query_hash = string.Create(CultureInfo.InvariantCulture, $"0x{i}"), query_text = new string((char)('a' + i), textLength) });
        var context = FindingMessageFormatter.BuildContext(finding, 1.5);
        var blocks = SlackBlocks(Slack(context));

        var at = blocks.FindIndex(b => SectionText(b)?.StartsWith("*Top Cpu Queries*", StringComparison.Ordinal) == true);
        var sections = new List<string>();
        for (var i = at; i < blocks.Count && SectionText(blocks[i]) is { } t; i++)
        {
            sections.Add(t);
        }

        Assert.Equal(expectedSections, sections.Count);
        Assert.All(sections, s => Assert.True(s.Length <= TextObjectLimit, $"{s.Length} chars"));
        Assert.Single(sections, s => s.StartsWith("*Top Cpu Queries*", StringComparison.Ordinal));
        /* Each record's whole text is inside exactly one section. */
        for (var i = 0; i < 3; i++)
        {
            var text = new string((char)('a' + i), textLength);
            Assert.Single(sections, s => s.Contains(text, StringComparison.Ordinal));
        }
    }

    /// <summary>
    /// A text past the record's room is cut with the count stated, in the words a cut field uses, and the
    /// cut lands on a whole character: a 🔥 astride the boundary is left out whole, no U+FFFD reaches the
    /// reader, and kept plus omitted is the text's own character count. The section stays inside every
    /// Slack ceiling. No producer emits such a text (the collectors bound them at 500); this is the ceiling's
    /// hygiene, pinned so it cannot cut silently.
    /// </summary>
    [Fact]
    public void Slack_CutsAnOversizedRecordText_StatingTheCountInCharacters_OnAWholeCharacter()
    {
        /* Control: an ASCII text of the same length reads the builder's own kept length K from the payload. */
        const int length = 6000;
        static string Rendered(string text)
        {
            var finding = HighCpuFinding(1, _ => new { database = "D", query_hash = "0x1", query_text = text });
            var blocks = SlackBlocks(Slack(FindingMessageFormatter.BuildContext(finding, 1.5)));
            var section = Assert.Single(blocks, b => SectionText(b)?.StartsWith("*Top Cpu Queries*", StringComparison.Ordinal) == true);
            var s = SectionText(section)!;
            Assert.True(s.Length <= TextObjectLimit, $"{s.Length} chars");
            return s;
        }

        static (string Kept, int Omitted) Cut(string section)
        {
            var fence = section.IndexOf("```\n", StringComparison.Ordinal) + 4;
            var noteAt = section.IndexOf("... (", fence, StringComparison.Ordinal);
            Assert.True(noteAt > fence, "no stated cut in the record text");
            var omitted = int.Parse(section[(noteAt + 5)..section.IndexOf(" more characters", noteAt, StringComparison.Ordinal)], NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
            return (section[fence..noteAt], omitted);
        }

        var control = Rendered(new string('x', length));
        var (controlKept, controlOmitted) = Cut(control);
        Assert.Equal(length, controlKept.Length + controlOmitted);
        Assert.Contains(Pointer, control, StringComparison.Ordinal);
        var k = controlKept.Length;

        /* The character under test straddles the old cut: units K-1..K. */
        const string fire = "\U0001F525";
        var value = new string('x', k - 1) + fire + new string('x', length - k - 1);
        Assert.Equal(length, value.Length);
        var section = Rendered(value);
        var (kept, omitted) = Cut(section);

        Assert.DoesNotContain('\uFFFD', section);
        Assert.Equal(new string('x', k - 1), kept);
        Assert.Equal(new StringInfo(value).LengthInTextElements - new StringInfo(kept).LengthInTextElements, omitted);
    }

    /// <summary>A record with two texts leads each with its label in italics so the reader can tell blocked
    /// from blocking; a record with one text carries no label — the heading and the summary already say
    /// what it is.</summary>
    [Fact]
    public void Slack_LabelsTextsOnlyWhenARecordHasSeveral()
    {
        var finding = HighCpuFinding(1, _ => new { database = "D", blocked_sql = "UPDATE t SET x = 1", blocking_sql = "SELECT * FROM t" });
        var section = Assert.Single(SlackBlocks(Slack(FindingMessageFormatter.BuildContext(finding, 1.5))), b => SectionText(b)?.StartsWith("*Top Cpu Queries*", StringComparison.Ordinal) == true);
        Assert.Equal("*Top Cpu Queries*\n*#1 · Database: D*\n_Blocked Sql_\n```\nUPDATE t SET x = 1\n```\n_Blocking Sql_\n```\nSELECT * FROM t\n```", SectionText(section));

        var one = Assert.Single(SlackBlocks(Slack(FindingMessageFormatter.BuildContext(HighCpuFinding(1), 1.5))), b => SectionText(b)?.StartsWith("*Top Cpu Queries*", StringComparison.Ordinal) == true);
        Assert.DoesNotContain("_Query Text_", SectionText(one), StringComparison.Ordinal);
    }

    /* ---------------- email and Teams ---------------- */

    /// <summary>Email HTML: one data row per record labelled <c>#N</c> with the summary, then a query row
    /// per text in the monospace <c>&lt;pre&gt;</c> idiom — six table rows for three queries instead of
    /// twenty-one, no <c>#1 Database</c> label anywhere. Plain text: the same list, the text indented under
    /// its label.</summary>
    [Fact]
    public void Email_RendersRecordsAsACompactList_InBothBodies()
    {
        var context = FindingMessageFormatter.BuildContext(HighCpuFinding(), 1.5);
        var item = Assert.Single(context.Details, d => d.Heading == "Top Cpu Queries");
        var (html, text) = Email(context);

        Assert.Contains(">#1</td>", html, StringComparison.Ordinal);
        Assert.Contains(System.Net.WebUtility.HtmlEncode(item.Records[0].Summary), html, StringComparison.Ordinal);
        Assert.Contains(">Query Text</td>", html, StringComparison.Ordinal);
        Assert.Contains("<pre style=\"margin:0;font-family:'Courier New',Consolas,monospace;font-size:12px;color:#E0E0E0;white-space:pre-wrap;word-break:break-all;\">" + System.Net.WebUtility.HtmlEncode(QueryText(0)) + "</pre>", html, StringComparison.Ordinal);
        Assert.DoesNotContain("#1 Database", html, StringComparison.Ordinal);
        Assert.Equal(3, CountOf(html, ">Query Text</td>"));

        Assert.Contains($"  #1: {item.Records[0].Summary}\r\n    Query Text:\r\n      {QueryText(0)}\r\n", text, StringComparison.Ordinal);
        Assert.DoesNotContain("#1 Database", text, StringComparison.Ordinal);
    }

    /// <summary>Teams: one fact per record — name <c>#N</c>, value the summary over the text in inline code
    /// — instead of one per attribute; three facts for three queries.</summary>
    [Fact]
    public void Teams_RendersRecordsAsOneFactPerRow()
    {
        var context = FindingMessageFormatter.BuildContext(HighCpuFinding(), 1.5);
        var item = Assert.Single(context.Details, d => d.Heading == "Top Cpu Queries");
        using var doc = JsonDocument.Parse(Teams(context));

        var section = Assert.Single(doc.RootElement.GetProperty("sections").EnumerateArray(),
            s => s.TryGetProperty("activityTitle", out var t) && t.GetString() == "Top Cpu Queries");
        var facts = section.GetProperty("facts").EnumerateArray().ToList();
        Assert.Equal(3, facts.Count);
        Assert.Equal(new[] { "#1", "#2", "#3" }, facts.Select(f => f.GetProperty("name").GetString()));
        Assert.Equal(item.Records[0].Summary + "  \n`" + QueryText(0) + "`", facts[0].GetProperty("value").GetString());
    }

    /* ---------------- byte identity ---------------- */

    /// <summary>
    /// Nothing that is not a record array changes by a byte on any surface. The context carries every other
    /// detail shape; each renderer's output equals the pre-#3644 rendering of the same context, restated
    /// here from the shapes the renderers have always emitted. The email stamps are normalized because
    /// <c>BuildAlertEmail</c> reads the clock.
    /// </summary>
    [Fact]
    public void NonRecordDetails_RenderByteIdentically_OnEverySurface()
    {
        var context = NonRecordContext();

        /* Slack: the pre-#3644 details loop, verbatim (the #3612 oracle's shapes). */
        var expectedSlack = new List<object>();
        foreach (var detail in context.Details)
        {
            expectedSlack.Add(new { type = "divider" });
            if (detail.IsCodeBlock)
            {
                expectedSlack.Add(new { type = "section", text = new { type = "mrkdwn", text = $"*{detail.Heading}*\nSee email or in-app Alert Details for the copy-paste T-SQL." } });
                continue;
            }

            if (!string.IsNullOrEmpty(detail.Body))
            {
                expectedSlack.Add(new { type = "section", text = new { type = "mrkdwn", text = $"*{detail.Heading}*\n{detail.Body}" } });
                continue;
            }

            var fields = new List<object> { new { type = "mrkdwn", text = $"*{detail.Heading}*" } };
            foreach (var (label, value) in detail.Fields)
            {
                fields.Add(new { type = "mrkdwn", text = $"*{label}:*\n{value}" });
            }

            expectedSlack.Add(new { type = "section", fields });
        }

        var slackRun = JsonSerializer.Serialize(expectedSlack);
        Assert.Contains(slackRun[1..^1], Slack(context), StringComparison.Ordinal);

        /* Teams: one fact per field, per detail section. */
        using var teams = JsonDocument.Parse(Teams(context));
        var chain = Assert.Single(teams.RootElement.GetProperty("sections").EnumerateArray(), s => s.TryGetProperty("activityTitle", out var t) && t.GetString() == "Blocking Chain 1");
        Assert.Equal(context.Details[3].Fields.Select(f => (f.Label, f.Value)),
            chain.GetProperty("facts").EnumerateArray().Select(f => (f.GetProperty("name").GetString()!, f.GetProperty("value").GetString()!)));

        /* Email: the fields table, label column then value, the SQL-labelled ones in <pre>. */
        var (html, text) = Email(context);
        Assert.Contains(">Blocked SQL</td>", html, StringComparison.Ordinal);
        Assert.Contains("<pre style=\"margin:0;font-family:'Courier New',Consolas,monospace;font-size:12px;color:#E0E0E0;white-space:pre-wrap;word-break:break-all;\">UPDATE dbo.Orders SET x = 1 WHERE id = 5</pre>", html, StringComparison.Ordinal);
        Assert.Contains("  Blocking Chain 1\r\n  Database: SalesDB\r\n  Blocked SQL: UPDATE dbo.Orders SET x = 1 WHERE id = 5\r\n  Blocking SQL: SELECT * FROM dbo.Orders WITH (HOLDLOCK)\r\n  Wait Time Ms: 12400\r\n", text, StringComparison.Ordinal);
        Assert.Contains("  Objects\r\n  #1: dbo.Orders\r\n  #2: dbo.OrderLines\r\n", text, StringComparison.Ordinal);
    }

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        for (var at = haystack.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = haystack.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
