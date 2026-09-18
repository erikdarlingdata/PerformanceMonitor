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
/// #3612: three analysis pages — five-fact stories rooted in a plan regression, the product's
/// highest-severity compound findings — died on Slack with <c>HTTP 400 invalid_attachments</c> across two
/// builds while a three-fact page delivered through the same webhook the same hour. #3493 had bounded the
/// alert's PROSE; the analysis pages deliver no prose at all and carry the whole finding as structured
/// details, and the details path had no size discipline of any kind. The monitoring seat's read of the
/// persisted <c>context_json</c> named the breach: every text object on the failed pages was under its cap
/// (a DELIVERED page carried the longest body), and the failed pages carried 19–21 detail items against
/// the largest delivered page's 18 — at roughly two blocks per item plus the fixed head and footer, the
/// fifty-block message limit.
///
/// <para><b>The fixture is the real shape, built through the shipped producer.</b>
/// <see cref="FiveFactPlanRegression"/> is the lost story with the drill-downs the Lite collector attaches
/// for each fact key on its path, sized as the collector sizes them (five rows, 500-character query text),
/// and it goes through <see cref="FindingMessageFormatter.BuildContext"/> — the same call the notification
/// service makes — so the items, their order and their field counts are the producer's, not a lookalike.
/// The only knob is how many distinct query hashes the drill-downs surface, because the formatter derives
/// one incident item per distinct hash and that is the count production varied on. Measured from source:
/// ten fixed items cost 33 blocks, the head 2, the footer 2, each incident 2 — 37 + 2×incidents — so six
/// incidents (49 blocks) is the last shape that fits and the seventh is the fifty-first block.</para>
///
/// <para><b>The two hazards this suite is built around.</b> A budget is trivially satisfied by dropping
/// things silently, so every over-budget arm asserts the omission item names the count AND every dropped
/// heading, and that the kept items are the leading ones in producer order (the finding, then the
/// drill-downs, then the incidents) — a reader who sees "Incident 5 of 11" followed by the omission item
/// knows exactly where the message stopped. And a bounding pass that changed the SMALL case would repaint
/// every engine alert and every analysis page that delivered before it, so the fitting shapes are pinned
/// against the pre-#3612 rendering byte for byte — the oracle is the old loop, restated here verbatim.</para>
/// </summary>
public class SlackDetailsSizeTests
{
    private static AlertBranding Branding => EmailAlertService.Branding;

    private const string TriageUrl = "https://example.invalid/triage/1";

    /* Slack's documented ceilings, restated so a drifted constant fails a test rather than resizing it. */
    private const int BlockLimit = 50;
    private const int TextObjectLimit = 3000;
    private const int FieldTextLimit = 2000;
    private const int FieldsPerSection = 10;

    private const string OmissionHeading = "*Omitted from this message*";
    private const string Pointer = "see email or in-app Alert Details for the full text";

    /* ---------------- the fixture ---------------- */

    private static string QueryText(int i) =>
        ("SELECT o.OrderId, o.CustomerId, o.OrderDate, ol.ProductId, ol.Quantity, ol.UnitPrice, p.ProductName, c.CustomerName "
         + "FROM Sales.Orders AS o JOIN Sales.OrderLines AS ol ON ol.OrderId = o.OrderId JOIN Warehouse.Products AS p ON p.ProductId = ol.ProductId "
         + "JOIN Sales.Customers AS c ON c.CustomerId = o.CustomerId WHERE o.OrderDate >= @StartDate AND o.OrderDate < @EndDate AND c.Region = @Region "
         + string.Create(CultureInfo.InvariantCulture, $"AND p.Category = @Category{i} ORDER BY o.OrderDate DESC, o.OrderId OPTION (RECOMPILE) -- variant {i} of the report family"))
        .Substring(0, 500);

    private static string Hash(int i) =>
        "0x" + (0x9A3F5C21D4E8B7A0UL + (ulong)i * 0x1F3D5B79UL).ToString("X16", CultureInfo.InvariantCulture);

    private static string PlanHash(int i) =>
        "0x" + (0x51C4E7A9B3D6F802UL + (ulong)i * 0x2E4C6A88UL).ToString("X16", CultureInfo.InvariantCulture);

    /// <summary>
    /// The lost story: PLAN_REGRESSION → CPU_SPIKE → PARAMETER_SENSITIVITY → QUERY_SPILLS → TEMPDB_USAGE,
    /// severity 1.75, confidence 0.80, with the seven drill-downs the collector attaches for those keys
    /// (spike_peak, queries_at_spike, top_cpu_queries, top_spilling_queries, tempdb_breakdown,
    /// parameter_sensitive_queries, regressed_queries) and the value-stated advice frozen into StoryText
    /// the way analysis freezes it. <paramref name="distinctHashes"/> is spread across the three
    /// hash-bearing drill-downs' fifteen row slots; the formatter derives one incident item per distinct
    /// hash.
    /// </summary>
    private static AnalysisFinding FiveFactPlanRegression(int distinctHashes)
    {
        var start = new DateTime(2026, 9, 18, 11, 40, 0, DateTimeKind.Utc);
        var end = new DateTime(2026, 9, 18, 15, 40, 0, DateTimeKind.Utc);

        var facts = new List<Fact>
        {
            new()
            {
                Key = "PLAN_REGRESSION", Value = 14.2, Severity = 0.9,
                Metadata = new()
                {
                    ["worst_regression_factor"] = 14.2, ["offender_count"] = 3,
                    ["latest_cpu_per_exec_us"] = 2_480_000, ["best_cpu_per_exec_us"] = 174_600,
                    ["latest_is_forced"] = 0, ["force_failure_count"] = 0
                }
            },
            new() { Key = "CPU_SPIKE", Value = 96.4, Severity = 0.8, Metadata = new() { ["peak_cpu"] = 96.4 } },
            new() { Key = "PARAMETER_SENSITIVITY", Value = 42.0, Severity = 0.6, Metadata = new() { ["worst_ratio"] = 42 } },
            new() { Key = "QUERY_SPILLS", Value = 1840, Severity = 0.5, Metadata = new() { ["total_spills"] = 1840 } },
            new() { Key = "TEMPDB_USAGE", Value = 61_440, Severity = 0.4, Metadata = new() { ["max_reserved_mb"] = 61_440 } },
        };
        var advice = FactAdvice.Compose("PLAN_REGRESSION", facts.ToFactLookup());

        string HashAt(int slot) => Hash(slot % distinctHashes);

        var queriesAtSpike = new List<object>();
        var topCpu = new List<object>();
        var spilling = new List<object>();
        var psp = new List<object>();
        var regressed = new List<object>();
        var tempdb = new List<object>();
        for (var i = 0; i < 5; i++)
        {
            queriesAtSpike.Add(new
            {
                time = start.AddMinutes(97 + i).ToString("o"),
                session_id = 120 + i * 7,
                database = "ReportingDB",
                status = "running",
                cpu_time_ms = 48_200L - i * 3_100,
                elapsed_time_ms = 61_900L - i * 2_800,
                logical_reads = 9_412_880L - i * 411_000,
                wait_type = i % 2 == 0 ? "CXPACKET" : "SOS_SCHEDULER_YIELD",
                dop = 8,
                parallel_workers = 16,
                query_text = QueryText(i)
            });
            topCpu.Add(new
            {
                database = "ReportingDB",
                query_hash = HashAt(i),
                total_cpu_ms = 1_284_400.0 - i * 90_000,
                execution_count = 3_120L - i * 400,
                max_dop = 8,
                spills = 412L - i * 60,
                query_text = QueryText(i)
            });
            spilling.Add(new
            {
                database = "ReportingDB",
                query_hash = HashAt(5 + i),
                total_spills = 640L - i * 90,
                execution_count = 2_040L - i * 300,
                query_text = QueryText(5 + i)
            });
            psp.Add(new
            {
                database = "ReportingDB",
                query_hash = HashAt(10 + i),
                query_plan_hash = PlanHash(10 + i),
                execution_count = 4_400L - i * 500,
                min_worker_time_us = 11_200L + i * 800,
                max_worker_time_us = 2_480_000L - i * 120_000,
                worker_ratio = 221.4 - i * 30.1,
                grant_ratio = 18.2 - i * 2.2,
                spills_on_some_inputs = i % 2 == 0,
                query_text = QueryText(10 + i)
            });
            regressed.Add(new
            {
                database = "ReportingDB",
                query_id = 48_213L + i * 17,
                latest_plan_hash = PlanHash(i),
                latest_cpu_per_exec_us = 2_480_000.0 - i * 200_000,
                latest_duration_per_exec_us = 3_912_000.0 - i * 250_000,
                best_plan_hash = PlanHash(20 + i),
                best_plan_id = 9_104L + i * 3,
                best_cpu_per_exec_us = 174_600.0 + i * 8_000,
                best_duration_per_exec_us = 233_900.0 + i * 9_000,
                regression_factor = 14.2 - i * 2.1,
                query_text = QueryText(20 + i),
                replica_role = "",
                parameter_sensitivity_cofired = i == 0
            });
            tempdb.Add(new
            {
                time = start.AddMinutes(95 + i * 5).ToString("o"),
                user_objects_mb = 1_204.5 + i * 12,
                internal_objects_mb = 58_112.0 - i * 900,
                version_store_mb = 96.25,
                unallocated_mb = 2_027.25 + i * 800
            });
        }

        return new AnalysisFinding
        {
            ServerId = 3,
            ServerName = "SQLPROD-A",
            Category = "query_regression",
            StoryPath = "PLAN_REGRESSION → CPU_SPIKE → PARAMETER_SENSITIVITY → QUERY_SPILLS → TEMPDB_USAGE",
            StoryPathHash = "5fact-planreg-0001",
            StoryText = FactAdvice.SerializeForStoryText(advice),
            Severity = 1.75,
            Confidence = 0.80,
            FactCount = 5,
            RootFactKey = "PLAN_REGRESSION",
            RootFactValue = 14.2,
            TimeRangeStart = start,
            TimeRangeEnd = end,
            DrillDown = new Dictionary<string, object>
            {
                ["spike_peak"] = new { time = start.AddMinutes(98).ToString("o"), cpu_percent = 96.4 },
                ["queries_at_spike"] = queriesAtSpike,
                ["top_cpu_queries"] = topCpu,
                ["top_spilling_queries"] = spilling,
                ["tempdb_breakdown"] = tempdb,
                ["parameter_sensitive_queries"] = psp,
                ["regressed_queries"] = regressed,
            }
        };
    }

    private static readonly string[] s_fixedHeadings =
    {
        "Diagnosis", "Remediation T-SQL", "Spike Peak", "Queries At Spike", "Top Cpu Queries",
        "Top Spilling Queries", "Tempdb Breakdown", "Parameter Sensitive Queries", "Regressed Queries",
    };

    /* ---------------- harness ---------------- */

    private static readonly DateTime s_now = new(2026, 9, 18, 15, 42, 0, DateTimeKind.Utc);

    private static string AnalysisPayload(AnalysisFinding finding, AlertContext context) =>
        WebhookAlertService.BuildSlackPayload(
            FindingMessageFormatter.MetricName(finding), finding.ServerName,
            FindingMessageFormatter.CurrentValue(finding), "1.5", Branding,
            context: context, triageUrl: TriageUrl, detailText: null, displayName: "Analysis finding", nowUtc: s_now);

    private static string Payload(AlertContext context, string? prose = null, string? triageUrl = null) =>
        WebhookAlertService.BuildSlackPayload(
            "Deadlocks Detected", "SQLPROD-A", "7", "1", Branding,
            context: context, triageUrl: triageUrl, detailText: prose, nowUtc: s_now);

    private static List<JsonElement> Blocks(JsonDocument doc) =>
        doc.RootElement.GetProperty("attachments")[0].GetProperty("blocks").EnumerateArray().ToList();

    private static string? SectionText(JsonElement block) =>
        block.GetProperty("type").GetString() == "section" && block.TryGetProperty("text", out var t)
            ? t.GetProperty("text").GetString()
            : null;

    private static IEnumerable<string> FieldTexts(JsonElement block) =>
        block.TryGetProperty("fields", out var fields)
            ? fields.EnumerateArray().Select(f => f.GetProperty("text").GetString()!)
            : Enumerable.Empty<string>();

    /// <summary>Slack's ceilings, all of them, asserted from the payload a reader was actually sent.</summary>
    private static void AssertInsideEveryCeiling(List<JsonElement> blocks)
    {
        Assert.True(blocks.Count <= BlockLimit, $"{blocks.Count} blocks");
        foreach (var block in blocks)
        {
            if (SectionText(block) is { } text)
            {
                Assert.True(text.Length <= TextObjectLimit, $"a section text is {text.Length} chars");
            }

            var fields = FieldTexts(block).ToList();
            if (block.TryGetProperty("fields", out _))
            {
                Assert.InRange(fields.Count, 1, FieldsPerSection);
            }

            Assert.All(fields, f => Assert.True(f.Length <= FieldTextLimit, $"a field text is {f.Length} chars"));
        }
    }

    /// <summary>Every mrkdwn text a reader was sent — section texts and field texts — parsed, because the raw
    /// payload JSON-escapes newlines and non-ASCII and a substring assertion against it would miss.</summary>
    private static List<string> AllTexts(List<JsonElement> blocks) =>
        blocks.SelectMany(b => FieldTexts(b).Concat(SectionText(b) is { } t ? new[] { t } : Array.Empty<string>())).ToList();

    private static string OmissionText(List<JsonElement> blocks) =>
        Assert.Single(blocks.Select(SectionText), t => t is not null && t.StartsWith(OmissionHeading, StringComparison.Ordinal))!;

    /// <summary>
    /// The pre-#3612 details loop, verbatim: divider, then a pointer section, a body section, or heading +
    /// fields in sections of ten. Serialized the way the builder serializes, this is the oracle every
    /// "fits" arm compares against — the bounding pass must not change a byte of a page that fits.
    /// </summary>
    private static string LegacyDetailBlocksJson(AlertContext context)
    {
        var blocks = new List<object>();
        foreach (var detail in context.Details)
        {
            blocks.Add(new { type = "divider" });
            if (detail.IsCodeBlock)
            {
                blocks.Add(new { type = "section", text = new { type = "mrkdwn", text = $"*{detail.Heading}*\nSee email or in-app Alert Details for the copy-paste T-SQL." } });
                continue;
            }

            if (!string.IsNullOrEmpty(detail.Body))
            {
                blocks.Add(new { type = "section", text = new { type = "mrkdwn", text = $"*{detail.Heading}*\n{detail.Body}" } });
                continue;
            }

            var fields = new List<object> { new { type = "mrkdwn", text = $"*{detail.Heading}*" } };
            foreach (var (label, value) in detail.Fields)
            {
                fields.Add(new { type = "mrkdwn", text = $"*{label}:*\n{value}" });
            }

            for (var i = 0; i < fields.Count; i += FieldsPerSection)
            {
                blocks.Add(new { type = "section", fields = fields.GetRange(i, Math.Min(FieldsPerSection, fields.Count - i)) });
            }
        }

        /* The list serializes as "[b1,b2,...]"; the inner run is what sits inside the payload's blocks array. */
        var json = JsonSerializer.Serialize(blocks);
        return json[1..^1];
    }

    private static int LegacyBlockCount(AlertContext context) =>
        JsonDocument.Parse("[" + LegacyDetailBlocksJson(context) + "]").RootElement.GetArrayLength();

    /* ---------------- the shape that must not change ---------------- */

    /// <summary>
    /// The regression pin, on the real producer: the lost story with three and with six distinct hashes
    /// — six is the LAST count that fits (49 blocks) — renders its details exactly as the pre-#3612 loop
    /// rendered them, byte for byte, with no omission item. Every analysis page that ever delivered lives
    /// on this path, and so does every engine alert.
    /// </summary>
    [Theory]
    [InlineData(3, 13, 43)]
    [InlineData(6, 16, 49)]
    public void AFiveFactStoryThatFits_RendersItsDetailsByteForByte_WithNoOmission(int distinctHashes, int expectedDetails, int expectedBlocks)
    {
        var finding = FiveFactPlanRegression(distinctHashes);
        var context = FindingMessageFormatter.BuildContext(finding, 1.5);
        Assert.Equal(expectedDetails, context.Details.Count);

        var payload = AnalysisPayload(finding, context);
        using var doc = JsonDocument.Parse(payload);
        var blocks = Blocks(doc);

        Assert.Equal(expectedBlocks, blocks.Count);
        Assert.Contains(LegacyDetailBlocksJson(context), payload, StringComparison.Ordinal);
        Assert.DoesNotContain(OmissionHeading, payload, StringComparison.Ordinal);
        AssertInsideEveryCeiling(blocks);
    }

    /* ---------------- the block budget and the stated omission ---------------- */

    /// <summary>
    /// The live failure and the fix. Seven distinct hashes is the first shape past the line (51 blocks
    /// under the old loop — the guard asserts it, so the arm can never pass vacuously); nine, ten and
    /// eleven are the three production pages (19, 20 and 21 details); fifteen is the most the three
    /// hash-bearing drill-downs can surface. Every one delivers inside every ceiling; the finding — the
    /// Diagnosis, the advice, the T-SQL pointer and all seven drill-downs — is whole; the incidents kept
    /// are the LEADING ones in order; and the omission item names how many were dropped and every one
    /// of them by heading.
    /// </summary>
    [Theory]
    [InlineData(7, 17)]
    [InlineData(9, 19)]
    [InlineData(10, 20)]
    [InlineData(11, 21)]
    [InlineData(15, 25)]
    public void AFiveFactStoryPastTheBlockLimit_DeliversInsideFiftyBlocks_WithTheOmissionStated(int distinctHashes, int expectedDetails)
    {
        var finding = FiveFactPlanRegression(distinctHashes);
        var context = FindingMessageFormatter.BuildContext(finding, 1.5);
        Assert.Equal(expectedDetails, context.Details.Count);

        /* Head (2) + footer (2) + the old loop's details must cross the limit, or this arm exercises nothing. */
        Assert.True(4 + LegacyBlockCount(context) > BlockLimit,
            $"the pre-#3612 rendering is only {4 + LegacyBlockCount(context)} blocks — this arm no longer reproduces the failure");

        var payload = AnalysisPayload(finding, context);
        using var doc = JsonDocument.Parse(payload);
        var blocks = Blocks(doc);

        AssertInsideEveryCeiling(blocks);
        var texts = AllTexts(blocks);

        /* The finding is whole: every fixed item, plus the advice headline, is in the message. */
        foreach (var heading in s_fixedHeadings)
        {
            Assert.Contains(texts, t => t.StartsWith($"*{heading}*", StringComparison.Ordinal));
        }

        Assert.Contains(texts, t => t.StartsWith($"*{context.Details[1].Heading}*\nInvestigation:", StringComparison.Ordinal));

        /* Incidents are kept as a prefix, in order: whichever is the first one missing, none after it is present. */
        var incidentHeadings = context.Details.Where(d => d.Heading.StartsWith("Incident ", StringComparison.Ordinal)).Select(d => d.Heading).ToList();
        var present = incidentHeadings.Select(h => texts.Contains($"*{h}*")).ToList();
        var firstMissing = present.IndexOf(false);
        Assert.True(firstMissing > 0, "at least the first incident must survive");
        Assert.All(present.Skip(firstMissing), p => Assert.False(p));

        /* The omission is stated: the count, every dropped heading, and where the whole page lives. */
        var dropped = incidentHeadings.Skip(firstMissing).ToList();
        var omission = OmissionText(blocks);
        Assert.Contains(
            string.Create(CultureInfo.InvariantCulture, $"{dropped.Count:N0} more details did not fit Slack's 50-block message limit"),
            omission, StringComparison.Ordinal);
        Assert.All(dropped, h => Assert.Contains(h, omission, StringComparison.Ordinal));
        Assert.Contains(Pointer, omission, StringComparison.Ordinal);

        /* The omission item is the LAST detail — after it come only the triage button and the footer. */
        var omissionIndex = blocks.FindIndex(b => SectionText(b)?.StartsWith(OmissionHeading, StringComparison.Ordinal) == true);
        Assert.Equal("divider", blocks[omissionIndex - 1].GetProperty("type").GetString());
        Assert.Equal("actions", blocks[omissionIndex + 1].GetProperty("type").GetString());
        Assert.Equal("context", blocks[omissionIndex + 2].GetProperty("type").GetString());
    }

    /// <summary>
    /// The budget is on BLOCKS, not on how many details there are: the same eighteen-item page fits or
    /// does not depending on how wide its items are. Eighteen two-block items fit with room to spare;
    /// eighteen items of which the last seven are four blocks wide do not, and the omission names exactly
    /// the ones that fell off. Production's clean 18-versus-19 separator was a property of those pages'
    /// widths, not a rule.
    /// </summary>
    [Fact]
    public void TheBudgetIsOnBlocks_NotOnTheDetailCount()
    {
        static AlertDetailItem Narrow(int i)
        {
            var item = new AlertDetailItem { Heading = string.Create(CultureInfo.InvariantCulture, $"Narrow {i}") };
            item.Fields.Add(("Dedup Key", "k"));
            return item;
        }

        static AlertDetailItem Wide(int i)
        {
            var item = new AlertDetailItem { Heading = string.Create(CultureInfo.InvariantCulture, $"Wide {i}") };
            for (var f = 0; f < 25; f++)
            {
                item.Fields.Add((string.Create(CultureInfo.InvariantCulture, $"f{f}"), "v"));
            }

            return item;
        }

        var narrow = new AlertContext();
        for (var i = 0; i < 18; i++)
        {
            narrow.Details.Add(Narrow(i));
        }

        var narrowPayload = Payload(narrow);
        Assert.DoesNotContain(OmissionHeading, narrowPayload, StringComparison.Ordinal);
        Assert.Contains(LegacyDetailBlocksJson(narrow), narrowPayload, StringComparison.Ordinal);

        var mixed = new AlertContext();
        for (var i = 0; i < 11; i++)
        {
            mixed.Details.Add(Narrow(i));
        }

        for (var i = 0; i < 7; i++)
        {
            mixed.Details.Add(Wide(i));
        }

        /* Head 2 + 11×2 + 7×4 + footer 1 = 53 under the old loop; the details' budget of 47, less the
           omission item's 2, holds the eleven narrow items and five of the wide ones (22 + 20 = 42, three
           blocks short of a sixth), so exactly two are dropped and the message is 47 blocks. */
        Assert.Equal(50, LegacyBlockCount(mixed));

        using var doc = JsonDocument.Parse(Payload(mixed));
        var blocks = Blocks(doc);
        Assert.Equal(47, blocks.Count);
        AssertInsideEveryCeiling(blocks);

        var omission = OmissionText(blocks);
        Assert.Contains("2 more details did not fit", omission, StringComparison.Ordinal);
        Assert.Contains("Wide 5; Wide 6", omission, StringComparison.Ordinal);
        var fieldTexts = blocks.SelectMany(FieldTexts).ToList();
        Assert.Contains("*Wide 4*", fieldTexts);
        Assert.DoesNotContain("*Wide 5*", fieldTexts);
    }

    /// <summary>
    /// A body detail AT the boundary is not dropped whole: it takes the blocks that remain and states its
    /// own line omission, because on an analysis page the body is the advice and a shortened advice beats
    /// a missing one. The details behind it are dropped and named. Both omissions are in the message, and
    /// it lands exactly on the cap.
    /// </summary>
    [Fact]
    public void ABodyDetailAtTheBoundary_TakesWhatRemains_AndStatesItsOwnOmission()
    {
        var context = new AlertContext();
        for (var i = 0; i < 20; i++)
        {
            var item = new AlertDetailItem { Heading = string.Create(CultureInfo.InvariantCulture, $"Incident {i + 1} of 20") };
            item.Fields.Add(("Dedup Key", "k"));
            context.Details.Add(item);
        }

        /* Head 2 + 20×2 = 42; footer 1 (no triage button); the details' budget is 47, so five blocks remain
           for this body's divider and sections after the omission item's two are charged. */
        var lines = Enumerable.Range(0, 5000).Select(i => string.Create(CultureInfo.InvariantCulture, $"advice-line-{i:D4} {new string('x', 40)}"));
        context.Details.Add(new AlertDetailItem { Heading = "Long advice", Body = string.Join('\n', lines) });
        context.Details.Add(new AlertDetailItem { Heading = "Trailing one", Body = "short" });
        context.Details.Add(new AlertDetailItem { Heading = "Trailing two", Body = "short" });

        var payload = Payload(context);
        using var doc = JsonDocument.Parse(payload);
        var blocks = Blocks(doc);

        Assert.Equal(BlockLimit, blocks.Count);
        AssertInsideEveryCeiling(blocks);

        var bodyTexts = blocks.Select(SectionText).Where(t => t is not null).ToList();
        var first = Assert.Single(bodyTexts, t => t!.StartsWith("*Long advice*\nadvice-line-0000", StringComparison.Ordinal));
        Assert.NotNull(first);
        Assert.Contains(bodyTexts, t => t!.Contains("first omitted: \"advice-line-", StringComparison.Ordinal));

        var omission = OmissionText(blocks);
        Assert.Contains("2 more details did not fit", omission, StringComparison.Ordinal);
        Assert.Contains("Trailing one; Trailing two", omission, StringComparison.Ordinal);
    }

    /// <summary>
    /// The prose and the details cannot collide into a fifty-first block: when both are heavy the details
    /// hold one block back for the prose, the prose packs into what remains, both state their omissions,
    /// and the message lands exactly on the cap. Before #3612 the prose's floor of one block was applied
    /// AFTER the details had spent the whole budget, which is a fifty-first block.
    /// </summary>
    [Fact]
    public void HeavyProseAndHeavyDetails_BothDegradeStated_AndNeverExceedFifty()
    {
        var context = new AlertContext();
        for (var i = 0; i < 30; i++)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture, $"Incident {i + 1} of 30"),
                Body = "Investigation: look at the graph.\n\nRemediation: stop looking at the graph."
            });
        }

        var prose = string.Join('\n', Enumerable.Range(0, 5000).Select(i => string.Create(CultureInfo.InvariantCulture, $"line-{i:D4} {new string('y', 50)}")));
        var payload = Payload(context, prose, TriageUrl);
        using var doc = JsonDocument.Parse(payload);
        var blocks = Blocks(doc);

        Assert.Equal(BlockLimit, blocks.Count);
        AssertInsideEveryCeiling(blocks);
        var texts = AllTexts(blocks);
        Assert.Contains(texts, t => t.StartsWith("*Details*\nline-0000", StringComparison.Ordinal));
        Assert.Contains(texts, t => t.Contains("first omitted: \"line-", StringComparison.Ordinal));
        Assert.Contains("more details did not fit", OmissionText(blocks), StringComparison.Ordinal);
        Assert.Contains("Open triage page", payload, StringComparison.Ordinal);
    }

    /* ---------------- the per-text-object caps (hygiene; production never touched them) ---------------- */

    /// <summary>
    /// A body past the text-object ceiling splits under its heading through the SAME splitter the prose
    /// uses — the heading leads the first section only, every section fits the cap, and re-joining the
    /// sections reproduces the body exactly. Hygiene: the longest body any analysis page has carried is
    /// 2,835 characters, and that one delivered.
    /// </summary>
    [Fact]
    public void ABodyPastTheTextObjectCeiling_SplitsUnderItsHeading_AndReassembles()
    {
        var body = string.Join('\n', Enumerable.Range(0, 120).Select(i => string.Create(CultureInfo.InvariantCulture, $"para-{i:D3} {new string('z', 60)}")));
        Assert.True(body.Length > TextObjectLimit);

        var context = new AlertContext();
        context.Details.Add(new AlertDetailItem { Heading = "Advice", Body = body });

        using var doc = JsonDocument.Parse(Payload(context));
        var blocks = Blocks(doc);
        AssertInsideEveryCeiling(blocks);

        var start = blocks.FindIndex(b => SectionText(b)?.StartsWith("*Advice*\n", StringComparison.Ordinal) == true);
        Assert.True(start > 0);
        var texts = new List<string>();
        for (var i = start; i < blocks.Count && SectionText(blocks[i]) is { } t; i++)
        {
            texts.Add(t);
        }

        Assert.True(texts.Count >= 3, "7,000+ chars cannot fit two sections");
        Assert.Equal(body, string.Join('\n', texts.Select((t, i) => i == 0 ? t["*Advice*\n".Length..] : t)));
    }

    /// <summary>
    /// A field value past the field ceiling keeps its leading stretch and states the cut inline — how many
    /// characters were omitted and where the whole value lives — inside the field's own 2,000. Fields
    /// under the ceiling render unchanged.
    /// </summary>
    [Fact]
    public void AFieldPastTheFieldCeiling_IsCutWithTheOmissionStatedInline()
    {
        var value = string.Concat(Enumerable.Range(0, 500).Select(i => string.Create(CultureInfo.InvariantCulture, $"{i:D4}-06789")));
        Assert.Equal(5000, value.Length);

        var context = new AlertContext();
        var item = new AlertDetailItem { Heading = "Regressed Queries" };
        item.Fields.Add(("#1 Query Text", value));
        item.Fields.Add(("#1 Database", "ReportingDB"));
        context.Details.Add(item);

        using var doc = JsonDocument.Parse(Payload(context));
        var blocks = Blocks(doc);
        AssertInsideEveryCeiling(blocks);

        var fields = blocks.SelectMany(FieldTexts).ToList();
        var cut = Assert.Single(fields, f => f.StartsWith("*#1 Query Text:*\n0000-06789", StringComparison.Ordinal));
        Assert.True(cut.Length <= FieldTextLimit);
        Assert.Contains("more characters - see email or in-app Alert Details for the full text)", cut, StringComparison.Ordinal);

        /* The stated count is the truth: kept + omitted = the original value. */
        var kept = cut["*#1 Query Text:*\n".Length..cut.IndexOf("... (", StringComparison.Ordinal)];
        var omitted = int.Parse(cut[(cut.IndexOf("... (", StringComparison.Ordinal) + 5)..cut.IndexOf(" more characters", StringComparison.Ordinal)], NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
        Assert.Equal(value.Length, kept.Length + omitted);
        Assert.Equal(value[..kept.Length], kept);

        Assert.Contains("*#1 Database:*\nReportingDB", fields);
    }

    /* ---------------- Teams ---------------- */

    /// <summary>
    /// The same lost page on Teams: the O365 MessageCard renders one section per field-bearing detail
    /// with no per-section count limit, and the connector caps the payload at 28 KB. The heaviest shape
    /// the story can produce (fifteen incidents, 25 items) is measured here against that cap, so the
    /// decision to ship no Teams change is a number rather than a hope — and a producer that widens the
    /// page past it fails this pin instead of a delivery.
    /// </summary>
    [Fact]
    public void TheHeaviestFiveFactPage_StaysInsideTheTeamsPayloadCap()
    {
        var finding = FiveFactPlanRegression(15);
        var context = FindingMessageFormatter.BuildContext(finding, 1.5);
        Assert.Equal(25, context.Details.Count);

        var teams = WebhookAlertService.BuildTeamsPayload(
            FindingMessageFormatter.MetricName(finding), finding.ServerName,
            FindingMessageFormatter.CurrentValue(finding), "1.5", Branding,
            context: context, triageUrl: TriageUrl, displayName: "Analysis finding", nowUtc: s_now);

        var bytes = Encoding.UTF8.GetByteCount(teams);
        Assert.True(bytes < 28 * 1024, $"the Teams card is {bytes} bytes against the connector's 28 KB cap");
    }
}
