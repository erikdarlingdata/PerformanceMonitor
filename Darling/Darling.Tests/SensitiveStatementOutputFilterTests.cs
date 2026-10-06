/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348: the MCP output filter, run through its real call-tool filter around a fake tool, so what is under test is
/// what a client would receive: which strings come back as the marker, where the marker sits in the structure, that
/// an error result is swept, how long a result that tries to run the judge out of time takes, and that the real
/// plan analyzer's output is withheld for the canary statement and kept for the plain one.
/// </summary>
public sealed class SensitiveStatementOutputFilterTests
{
    private readonly ITestOutputHelper _output;

    public SensitiveStatementOutputFilterTests(ITestOutputHelper output) => _output = output;

    private const string Marker = SensitiveStatements.PlaceholderText;

    private static readonly string s_plainRow =
        new JsonObject { ["query_text"] = StatementScrubCanary.PlainStatement, ["rows"] = 3 }.ToJsonString();

    /// <summary>Runs <paramref name="result"/> through the real filter as a tool's return value.</summary>
    private static async Task<CallToolResult> ThroughFilterAsync(CallToolResult result)
    {
        McpRequestHandler<CallToolRequestParams, CallToolResult> handler =
            SensitiveStatementOutputFilter.Instance((_, _) => ValueTask.FromResult(result));
        return await handler(null!, CancellationToken.None);
    }

    private static async Task<McpException> ThrownThroughFilterAsync(McpException thrown)
    {
        McpRequestHandler<CallToolRequestParams, CallToolResult> handler =
            SensitiveStatementOutputFilter.Instance((_, _) => throw thrown);
        return await Assert.ThrowsAnyAsync<McpException>(async () => await handler(null!, CancellationToken.None));
    }

    [Fact]
    public async Task AThrownMcpException_IsRethrownWithItsMessageSwept_KeepingTypeCodeAndInnerException()
    {
        // L1: the SDK writes a thrown McpException's message into an error result outside the result sweep
        var inner = new InvalidOperationException("inner");
        var plain = await ThrownThroughFilterAsync(new McpException("server_name is required", inner));
        Assert.Equal("server_name is required", plain.Message);

        var swept = await ThrownThroughFilterAsync(new McpException(StatementScrubCanary.CanaryStatement, inner));
        Assert.Equal(typeof(McpException), swept.GetType());
        Assert.Same(inner, swept.InnerException);
        Assert.Equal(Marker, swept.Message);

        var protocol = await ThrownThroughFilterAsync(
            new McpProtocolException(StatementScrubCanary.CanaryStatement, inner, McpErrorCode.InvalidParams));
        var rethrown = Assert.IsType<McpProtocolException>(protocol);
        Assert.Equal(McpErrorCode.InvalidParams, rethrown.ErrorCode);
        Assert.Same(inner, rethrown.InnerException);
        Assert.Equal(Marker, rethrown.Message);
    }

    private static CallToolResult Text(string text, bool? isError = null) => new()
    {
        Content = new List<ContentBlock> { new TextContentBlock { Text = text } },
        IsError = isError,
    };

    private static string TextOf(CallToolResult r) => ((TextContentBlock)r.Content[0]).Text;

    private static string Json(params (string Key, string Value)[] pairs)
    {
        var obj = new JsonObject();
        foreach (var (key, value) in pairs) obj[key] = value;
        return obj.ToJsonString();
    }

    // ── precise: the marker lands inside the structure, plain text is untouched ──

    [Fact]
    public async Task ACanaryStatementBecomesTheMarkerInsideTheStructure_APlainOneIsUnchanged()
    {
        string json = Json(
            ("named", StatementScrubCanary.CanaryStatement),
            ("plain", StatementScrubCanary.PlainStatement));

        var result = await ThroughFilterAsync(Text(json));

        var parsed = JsonNode.Parse(TextOf(result))!.AsObject();
        Assert.Equal(Marker, (string?)parsed["named"]);
        Assert.Equal(StatementScrubCanary.PlainStatement, (string?)parsed["plain"]);
        Assert.Equal(2, parsed.Count);
        Assert.DoesNotContain("S3cret-canary-ssf", TextOf(result));
    }

    [Fact]
    public async Task ACreateAndLoginSplitByALineFeedIsWithheldToo()
    {
        string statement = "CREATE\nLOGIN [canary_lf_ssf] WITH PASSWORD = N'S3cret-lf-ssf'";
        string json = Json(("sql_text", statement), ("note", "second value kept as is"));

        var result = await ThroughFilterAsync(Text(json));

        var parsed = JsonNode.Parse(TextOf(result))!.AsObject();
        Assert.Equal(Marker, (string?)parsed["sql_text"]);
        Assert.Equal("second value kept as is", (string?)parsed["note"]);
        Assert.DoesNotContain("S3cret-lf-ssf", TextOf(result));
    }

    // ── blocks the filter cannot read are refused, and every block's _meta is swept ──

    private static void AssertTheFixedRefusal(CallToolResult result)
    {
        Assert.True(result.IsError);
        Assert.Single(result.Content);
        Assert.Equal(SensitiveStatements.JsonRefusal, TextOf(result));
        Assert.Null(result.StructuredContent);
    }

    [Fact]
    public async Task ABlobResourceBlockBecomesTheFixedRefusal()
    {
        var blob = new BlobResourceContents
        {
            Uri = "file:///x",
            MimeType = "text/plain",
            Blob = System.Text.Encoding.UTF8.GetBytes(StatementScrubCanary.CanaryStatement),
        };
        var original = new CallToolResult
        {
            Content = new List<ContentBlock>
            {
                new TextContentBlock { Text = s_plainRow },
                new EmbeddedResourceBlock { Resource = blob },
            },
        };

        AssertTheFixedRefusal(await ThroughFilterAsync(original));
    }

    [Fact]
    public async Task AResourceLinkBlockBecomesTheFixedRefusal()
    {
        var original = new CallToolResult
        {
            Content = new List<ContentBlock>
            {
                new ResourceLinkBlock
                {
                    Uri = "file:///x",
                    Name = StatementScrubCanary.CanaryStatement,
                    Description = StatementScrubCanary.CanaryStatement,
                },
            },
        };

        var result = await ThroughFilterAsync(original);

        AssertTheFixedRefusal(result);
        Assert.DoesNotContain("S3cret-canary-ssf", TextOf(result));
    }

    [Fact]
    public async Task ATextBlockCarryingACanaryInItsMetaHasTheCanaryWithheld()
    {
        var original = new CallToolResult
        {
            Content = new List<ContentBlock>
            {
                new TextContentBlock
                {
                    Text = s_plainRow,
                    Meta = new JsonObject { ["note"] = StatementScrubCanary.CanaryStatement, ["keep"] = "plain" },
                },
            },
        };

        var result = await ThroughFilterAsync(original);

        var block = Assert.IsType<TextContentBlock>(result.Content[0]);
        Assert.Equal(s_plainRow, block.Text);
        Assert.Equal(Marker, (string?)block.Meta!["note"]);
        Assert.Equal("plain", (string?)block.Meta["keep"]);
        Assert.DoesNotContain("S3cret-canary-ssf", block.Meta.ToJsonString());
    }

    [Fact]
    public async Task ACanaryInAnEmbeddedTextResourcesMetaOrTheResultsMetaIsWithheld()
    {
        var original = new CallToolResult
        {
            Content = new List<ContentBlock>
            {
                new EmbeddedResourceBlock
                {
                    Meta = new JsonObject { ["a"] = StatementScrubCanary.CanaryStatement },
                    Resource = new TextResourceContents
                    {
                        Uri = "file:///x",
                        Text = s_plainRow,
                        Meta = new JsonObject { ["b"] = StatementScrubCanary.CanaryStatement },
                    },
                },
            },
            Meta = new JsonObject { ["c"] = StatementScrubCanary.CanaryStatement },
        };

        var result = await ThroughFilterAsync(original);

        var embedded = Assert.IsType<EmbeddedResourceBlock>(result.Content[0]);
        var resource = Assert.IsType<TextResourceContents>(embedded.Resource);
        Assert.Equal(Marker, (string?)embedded.Meta!["a"]);
        Assert.Equal(Marker, (string?)resource.Meta!["b"]);
        Assert.Equal(Marker, (string?)result.Meta!["c"]);
        Assert.Equal(s_plainRow, resource.Text);
    }

    // A URI or a MIME type that carries a named statement is useless once the statement is replaced, so the
    // whole result becomes the fixed refusal (the block keeps a valid shape that way).

    [Fact]
    public async Task ACanaryInAnEmbeddedTextResourcesUriBecomesTheFixedRefusal()
    {
        var original = new CallToolResult
        {
            Content = new List<ContentBlock>
            {
                new EmbeddedResourceBlock
                {
                    Resource = new TextResourceContents
                    {
                        Uri = "x:" + StatementScrubCanary.CanaryStatement,
                        MimeType = "text/plain",
                        Text = "plain row",
                    },
                },
            },
        };

        var result = await ThroughFilterAsync(original);

        AssertTheFixedRefusal(result);
        Assert.DoesNotContain("S3cret-canary-ssf", TextOf(result));
    }

    [Fact]
    public async Task ACanaryInAnEmbeddedTextResourcesMimeTypeBecomesTheFixedRefusal()
    {
        var original = new CallToolResult
        {
            Content = new List<ContentBlock>
            {
                new EmbeddedResourceBlock
                {
                    Resource = new TextResourceContents
                    {
                        Uri = "file:///x",
                        MimeType = StatementScrubCanary.CanaryStatement,
                        Text = "plain row",
                    },
                },
            },
        };

        var result = await ThroughFilterAsync(original);

        AssertTheFixedRefusal(result);
        Assert.DoesNotContain("S3cret-canary-ssf", TextOf(result));
    }

    [Fact]
    public async Task AnEmbeddedTextResourceWithAPlainUriAndMimeTypeKeepsTheResultAsTheSameInstance()
    {
        var original = new CallToolResult
        {
            Content = new List<ContentBlock>
            {
                new EmbeddedResourceBlock
                {
                    Resource = new TextResourceContents
                    {
                        Uri = "file:///x",
                        MimeType = "text/plain",
                        Text = "plain row",
                    },
                },
            },
        };

        var result = await ThroughFilterAsync(original);

        Assert.Same(original, result);
    }

    [Fact]
    public async Task AMetaThatNamesNothingKeepsTheResultAsTheSameInstance()
    {
        var original = new CallToolResult
        {
            Content = new List<ContentBlock>
            {
                new TextContentBlock { Text = s_plainRow, Meta = new JsonObject { ["k"] = "v" } },
            },
            Meta = new JsonObject { ["k"] = "v" },
        };

        Assert.Same(original, await ThroughFilterAsync(original));
    }

    [Fact]
    public async Task AResultThatNamesNothingComesBackAsTheSameInstance()
    {
        var original = Text(s_plainRow);

        var result = await ThroughFilterAsync(original);

        Assert.Same(original, result);
        Assert.Same(s_plainRow, TextOf(result));
    }

    [Fact]
    public async Task AnErrorResultIsSweptAndStaysAnError()
    {
        string json = Json(("error", "failed near: " + StatementScrubCanary.CanaryStatement));

        var result = await ThroughFilterAsync(Text(json, isError: true));

        Assert.True(result.IsError);
        Assert.DoesNotContain("S3cret-canary-ssf", TextOf(result));
        Assert.Contains(Marker, TextOf(result));
    }

    [Fact]
    public async Task APlainTextErrorSentenceIsSwept()
    {
        var result = await ThroughFilterAsync(
            Text("could not run: " + StatementScrubCanary.CanaryStatement, isError: true));

        Assert.True(result.IsError);
        Assert.Equal(Marker, TextOf(result));
    }

    [Fact]
    public async Task EveryTextBlockOfAResultIsSwept_AndTheOtherFieldsCarryOver()
    {
        var original = new CallToolResult
        {
            Content = new List<ContentBlock>
            {
                new TextContentBlock { Text = s_plainRow },
                new TextContentBlock { Text = Json(("q", StatementScrubCanary.CanaryStatement)) },
            },
            IsError = false,
        };

        var result = await ThroughFilterAsync(original);

        Assert.Equal(2, result.Content.Count);
        Assert.Same(s_plainRow, ((TextContentBlock)result.Content[0]).Text);
        Assert.DoesNotContain("S3cret-canary-ssf", ((TextContentBlock)result.Content[1]).Text);
        Assert.False(result.IsError);
    }

    [Fact]
    public async Task StructuredContentIsSweptToo()
    {
        using var doc = JsonDocument.Parse(Json(("q", StatementScrubCanary.CanaryStatement), ("n", "keep this value")));
        var original = Text(s_plainRow);
        original.StructuredContent = doc.RootElement.Clone();

        var result = await ThroughFilterAsync(original);

        Assert.NotNull(result.StructuredContent);
        string raw = result.StructuredContent!.Value.GetRawText();
        Assert.DoesNotContain("S3cret-canary-ssf", raw);
        Assert.Equal(Marker, result.StructuredContent.Value.GetProperty("q").GetString());
        Assert.Equal("keep this value", result.StructuredContent.Value.GetProperty("n").GetString());
    }

    [Fact]
    public async Task ANamedKeyBecomesTheMarkerWithItsPosition_AndItsValueIsKept()
    {
        string json = new JsonObject
        {
            ["id"] = "row one",
            [StatementScrubCanary.CanaryStatement] = "the value under a named key",
        }.ToJsonString();

        var result = await ThroughFilterAsync(Text(json));

        var parsed = JsonNode.Parse(TextOf(result))!.AsObject();
        Assert.Equal(2, parsed.Count);
        Assert.Equal("row one", (string?)parsed["id"]);
        Assert.Equal("the value under a named key", (string?)parsed[Marker + "#1"]);
        Assert.DoesNotContain("S3cret-canary-ssf", TextOf(result));
    }

    // ── the clock: a result built to run the judge out of time ──

    /// <summary>A near-hit the linear-time pre-check passes and the full judge times out on (see
    /// <c>SensitiveStatementsTests</c>): each costs one 250 ms match timeout until the budget is spent.</summary>
    private static IEnumerable<string> NearHits() =>
        SensitiveStatementCorpus.Adversarial.Select(adversarial => "xcreate login; " + adversarial);

    /// <summary>Every string value in every row is the marker. A key is judged like a value, so once the budget is
    /// spent a key is the marker plus its position; before that it is kept.</summary>
    private static void AssertEveryStringIsTheMarker(JsonArray rows)
    {
        foreach (var row in rows)
        {
            foreach (var pair in row!.AsObject())
            {
                Assert.True(pair.Key is "i" or "query_text" || pair.Key.StartsWith(Marker, StringComparison.Ordinal), pair.Key);
                if (pair.Value is JsonValue value && value.GetValueKind() == JsonValueKind.String)
                    Assert.Equal(Marker, value.GetValue<string>());
            }
        }
    }

    [Fact]
    public async Task AHundredValuesThatTimeOutFinishWithinTheBudgetAndEveryOneIsWithheld()
    {
        var hits = NearHits().ToArray();
        var rows = new JsonArray();
        for (int i = 0; i < 100; i++)
            rows.Add(new JsonObject { ["i"] = i, ["query_text"] = hits[i % hits.Length] });
        string json = rows.ToJsonString();
        SensitiveStatements.Judge("warm the regex up");
        await ThroughFilterAsync(Text(Json(("q", "warm the filter up"))));

        // The bound belongs to the code, not to the runner's load: assert the BEST of three runs and record the worst.
        double worst = 0;
        double best = double.MaxValue;
        CallToolResult? last = null;
        for (int attempt = 0; attempt < 3; attempt++)
        {
            var started = Stopwatch.GetTimestamp();
            last = await ThroughFilterAsync(Text(json));
            double ms = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
            worst = Math.Max(worst, ms);
            best = Math.Min(best, ms);
        }

        _output.WriteLine($"100-value case over 3 runs: worst {worst:F0} ms, best {best:F0} ms");
        Assert.True(best <= 1950, $"best elapsed {best:F0} ms (worst {worst:F0} ms)");
        var parsed = JsonNode.Parse(TextOf(last!))!.AsArray();
        Assert.Equal(100, parsed.Count);
        AssertEveryStringIsTheMarker(parsed);
    }

    [Fact]
    public async Task ABudgetSharedByTwoBlocksIsSpentOnceNotTwice()
    {
        var hits = NearHits().ToArray();
        var rows = new JsonArray();
        for (int i = 0; i < 50; i++)
            rows.Add(new JsonObject { ["query_text"] = hits[i % hits.Length] });
        string json = rows.ToJsonString();
        var original = new CallToolResult
        {
            Content = new List<ContentBlock>
            {
                new TextContentBlock { Text = json },
                new TextContentBlock { Text = json },
            },
        };
        SensitiveStatements.Judge("warm the regex up");

        // Best of three, worst recorded (see the 100-value case above).
        double worst = 0;
        double best = double.MaxValue;
        CallToolResult? result = null;
        for (int attempt = 0; attempt < 3; attempt++)
        {
            var started = Stopwatch.GetTimestamp();
            result = await ThroughFilterAsync(original);
            double ms = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
            worst = Math.Max(worst, ms);
            best = Math.Min(best, ms);
        }

        _output.WriteLine($"two blocks of 50 timing-out values over 3 runs: worst {worst:F0} ms, best {best:F0} ms");
        Assert.True(best <= 1950, $"best {best:F0} ms (worst {worst:F0} ms)");
        foreach (var block in result!.Content.Cast<TextContentBlock>())
            AssertEveryStringIsTheMarker(JsonNode.Parse(block.Text)!.AsArray());
    }

    /// <summary>Measurement for the lane report, not a gate: the sweep's cost on a 1 MB response that names
    /// nothing (200 rows of 5000 characters plus a plain statement each).</summary>
    [Fact]
    public async Task MeasureSweepOverheadOnAOneMegabyteResponseThatNamesNothing()
    {
        var rows = new JsonArray();
        string filler = string.Concat(Enumerable.Repeat("SELECT c FROM dbo.t WHERE id = @id; ", 140));
        for (int i = 0; i < 200; i++)
            rows.Add(new JsonObject { ["id"] = i, ["query_text"] = filler + i, ["database_name"] = "db" + i });
        string json = rows.ToJsonString();
        var original = Text(json);
        await ThroughFilterAsync(original);

        var times = new List<double>();
        for (int i = 0; i < 9; i++)
        {
            var started = Stopwatch.GetTimestamp();
            var result = await ThroughFilterAsync(original);
            times.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            Assert.Same(original, result);
        }

        times.Sort();
        _output.WriteLine($"{json.Length:N0} characters; median sweep {times[times.Count / 2]:F1} ms (min {times[0]:F1}, max {times[^1]:F1})");
        Assert.True(json.Length >= 1_000_000, $"{json.Length} characters");
    }

    // ── the real plan analyzer ──

    [Fact]
    public async Task AnalyzePlanXmlOnTheCanaryPlanWithholdsStatementOneAndKeepsStatementTwo()
    {
        // the control: the analysis of the unfiltered plan holds the parameter and constant values
        string control = McpPlanAnalysisFormatter.BuildAnalysisResult(
            StatementScrubCanary.CanaryPlan(), null, "xml", null, (AnalyzerConfig?)null, CancellationToken.None);
        Assert.Contains("param-secret-ssf", control);

        string analysis = DarlingMcpPlanTools.AnalyzePlanXml(StatementScrubCanary.CanaryPlan());
        var result = await ThroughFilterAsync(Text(analysis));
        string text = TextOf(result);

        _output.WriteLine(text.Length > 600 ? text.Substring(0, 600) : text);
        Assert.DoesNotContain("S3cret-canary-ssf", text);
        Assert.DoesNotContain("param-secret-ssf", text);
        Assert.DoesNotContain("const-secret-ssf", text);
        Assert.Contains(Marker, text);
        Assert.Contains("canary_plain_ssf", text);
        Assert.Contains("param-canary-ssf", text);
        JsonNode.Parse(text);
    }

    // ── the real XML read ──

    [Fact]
    public void TheCanaryPlanThroughTheRealXmlWithholdsEverySecretNeedleAndKeepsStatementTwoAndThree()
    {
        string plan = StatementScrubCanary.CanaryPlan();

        string? result = SensitiveStatements.Xml(plan);

        Assert.NotNull(result);
        Assert.NotSame(plan, result);
        foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, result);
        foreach (string needle in StatementScrubCanary.KeptNeedles) Assert.Contains(needle, result);
        var texts = XDocument.Parse(result!).Descendants().Where(e => e.Name.LocalName == "StmtSimple")
            .Select(e => (string?)e.Attribute("StatementText")).ToList();
        Assert.Equal(3, texts.Count);
        Assert.Equal(Marker, texts[0]);
        Assert.Equal(StatementScrubCanary.PlainStatement, texts[1]);
        Assert.Equal(StatementScrubCanary.AutoParamStatement, texts[2]);
    }

    [Theory]
    [InlineData("autoparam_update_prepared.xml")]
    [InlineData("autoparam_select_prepared.xml")]
    [InlineData("autoparam_update_adhoc_shell.xml")]
    [InlineData("autoparam_select_adhoc_shell.xml")]
    public void ACapturedPlanThroughTheRealXmlWithholdsTheLiteral(string file)
    {
        string xml = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "StatementScrub", file));
        Assert.Contains("S3cret-fixture", xml);

        string? result = SensitiveStatements.Xml(xml);

        Assert.NotNull(result);
        Assert.DoesNotContain("S3cret-fixture", result);
        XDocument.Parse(result!);
    }

    [Fact]
    public void ACapturedPreparedPlanKeepsItsStatementText()
    {
        string xml = File.ReadAllText(
            Path.Combine(AppContext.BaseDirectory, "Fixtures", "StatementScrub", "autoparam_update_prepared.xml"));
        string original = (string)XDocument.Parse(xml).Descendants().First(e => e.Attribute("StatementText") != null)
            .Attribute("StatementText")!;

        string? result = SensitiveStatements.Xml(xml);

        string kept = (string)XDocument.Parse(result!).Descendants().First(e => e.Attribute("StatementText") != null)
            .Attribute("StatementText")!;
        Assert.Equal(original, kept);
    }

    [Fact]
    public void XmlOnAPlanThatNamesNothingReturnsTheSameInstance_AndOnNullOrEmptyTheInput()
    {
        string plain = "<ShowPlanXML><a StatementText=\"" + StatementScrubCanary.PlainStatement + "\"/></ShowPlanXML>";

        Assert.Same(plain, SensitiveStatements.Xml(plain));
        Assert.Null(SensitiveStatements.Xml(null));
        Assert.Same(string.Empty, SensitiveStatements.Xml(string.Empty));
    }

    [Fact]
    public void JsonOnANonJsonSentenceJudgesItWhole_AndRefusesAnUnreadableOpener()
    {
        Assert.Equal(Marker, SensitiveStatements.Json("error in " + StatementScrubCanary.CanaryStatement));
        Assert.Equal(SensitiveStatements.JsonRefusal, SensitiveStatements.Json("{\"q\": \"" + StatementScrubCanary.CanaryStatement));
        Assert.Same("plain words", SensitiveStatements.Json("plain words"));
    }
}
