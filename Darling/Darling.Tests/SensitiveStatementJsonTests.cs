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
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Xml.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348: the JSON walk of the statement filter, run with fake judges so only the mechanics are under test: which
/// strings reach which judge, what a replaced key looks like, when the original instance comes back, and what
/// happens when a judge throws.
/// </summary>
public sealed class SensitiveStatementJsonTests
{
    private readonly ITestOutputHelper _output;

    public SensitiveStatementJsonTests(ITestOutputHelper output) => _output = output;

    private const string Marker = "[withheld]";
    private const string Refusal = "Output withheld: test refusal.";

    private sealed class Recorder
    {
        public readonly List<string> TextCalls = new();
        public readonly List<string> XmlCalls = new();

        public string? Text(string? s)
        {
            TextCalls.Add(s ?? "<null>");
            return s != null && s.Contains("SECRET", StringComparison.Ordinal) ? Marker : s;
        }

        public string? Xml(string? s)
        {
            XmlCalls.Add(s ?? "<null>");
            return s != null && s.Contains("SECRET", StringComparison.Ordinal) ? "<x/>" : s;
        }

        public string Run(string output) => SensitiveStatements.JsonCore(output, Text, Xml, Refusal);
    }

    [Fact]
    public void NestedObjectsAndArraysAreWalked()
    {
        var r = new Recorder();
        string output = "{\"a\":{\"b\":[\"first SECRET value\",{\"c\":\"keep this one\",\"d\":[\"deep SECRET text\"]}],\"n\":7},\"flag\":true,\"nothing\":null}";

        string result = r.Run(output);

        Assert.Equal(
            "{\"a\":{\"b\":[\"" + Marker + "\",{\"c\":\"keep this one\",\"d\":[\"" + Marker + "\"]}],\"n\":7},\"flag\":true,\"nothing\":null}",
            result);
    }

    [Fact]
    public void AngleFirstValuesGoToXmlAndOthersGoToText()
    {
        var r = new Recorder();
        string output = "{\"plan\":\"  <ShowPlanXML>SECRET</ShowPlanXML>\",\"short_xml\":\"<a/>\",\"stmt\":\"select SECRET from t\",\"tiny\":\"abcd\",\"number\":12345}";

        string result = r.Run(output);

        Assert.Equal(new[] { "  <ShowPlanXML>SECRET</ShowPlanXML>", "<a/>" }, r.XmlCalls);
        Assert.DoesNotContain(r.TextCalls, c => c.StartsWith("  <", StringComparison.Ordinal) || c == "<a/>");
        Assert.Contains("select SECRET from t", r.TextCalls);
        Assert.DoesNotContain("abcd", r.TextCalls);
        Assert.DoesNotContain("12345", r.TextCalls);
        var node = JsonNode.Parse(result)!.AsObject();
        Assert.Equal("<x/>", (string?)node["plan"]);
        Assert.Equal("<a/>", (string?)node["short_xml"]);
        Assert.Equal(Marker, (string?)node["stmt"]);
        Assert.Equal("abcd", (string?)node["tiny"]);
        Assert.Equal(12345, (int?)node["number"]);
    }

    [Fact]
    public void NamedKeyBecomesMarkerWithPositionAndKeysStayUnique()
    {
        var r = new Recorder();
        string output = "{\"keep\":1,\"SECRET_key_one\":\"v1\",\"SECRET_key_two\":\"v2\",\"plain_key\":\"v3\"}";

        string result = r.Run(output);

        Assert.Equal("{\"keep\":1,\"" + Marker + "#1\":\"v1\",\"" + Marker + "#2\":\"v2\",\"plain_key\":\"v3\"}", result);
        var keys = JsonNode.Parse(result)!.AsObject().Select(p => p.Key).ToList();
        Assert.Equal(keys.Count, keys.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public void ValueOfANamedKeyIsStillJudged()
    {
        var r = new Recorder();

        string result = r.Run("{\"SECRET_key_name\":\"also SECRET value\"}");

        Assert.Equal("{\"" + Marker + "#0\":\"" + Marker + "\"}", result);
    }

    [Fact]
    public void NamedKeysInNestedObjectsUseTheirOwnPosition()
    {
        var r = new Recorder();

        string result = r.Run("{\"x\":[{\"a\":1,\"b\":2,\"SECRET_nested_key\":3}]}");

        Assert.Equal("{\"x\":[{\"a\":1,\"b\":2,\"" + Marker + "#2\":3}]}", result);
    }

    [Fact]
    public void RootStringIsJudged()
    {
        var r = new Recorder();

        Assert.Equal("\"" + Marker + "\"", r.Run("\"root SECRET text\""));
    }

    [Fact]
    public void NothingChangedReturnsTheOriginalInstance()
    {
        var r = new Recorder();
        // Pretty-printed on purpose: unchanged output must not be re-serialized into the compact form.
        string output = "{\n  \"a\": [ \"nothing to see here\", 1, 2 ],\n  \"b\": { \"c\": \"<plan/>\" }\n}";

        string result = r.Run(output);

        Assert.Same(output, result);
    }

    [Fact]
    public void ChangedOutputIsReserializedWithTheMcpOptions()
    {
        var r = new Recorder();
        string output = "{\n  \"a\" : \"SECRET text here\",\n  \"b\" : [ 1, 2 ]\n}";

        string result = r.Run(output);

        Assert.Equal("{\"a\":\"" + Marker + "\",\"b\":[1,2]}", result);
    }

    [Fact]
    public void NonJsonIsJudgedWhole()
    {
        var r = new Recorder();

        string plainText = "Error: SECRET statement text, not json";
        Assert.Equal(Marker, r.Run(plainText));
        Assert.Equal(new[] { plainText }, r.TextCalls);
        Assert.Empty(r.XmlCalls);

        var r2 = new Recorder();
        string plan = "\r\n  <ShowPlanXML>SECRET</ShowPlanXML>";
        Assert.Equal("<x/>", r2.Run(plan));
        Assert.Equal(new[] { plan }, r2.XmlCalls);
        Assert.Empty(r2.TextCalls);
    }

    [Fact]
    public void NonJsonThatIsCleanComesBackAsTheSameInstance()
    {
        var r = new Recorder();
        string plain = "No server named that is registered.";
        string plan = "<ShowPlanXML>clean</ShowPlanXML>";

        Assert.Same(plain, r.Run(plain));
        Assert.Same(plan, r.Run(plan));
    }

    [Fact]
    public void AThrowingJudgeGivesExactlyTheRefusalArgument()
    {
        Func<string?, string?> boom = _ => throw new InvalidOperationException("judge failed");
        Func<string?, string?> ok = s => s;

        // text judge: a value, a key, and a whole non-JSON output.
        Assert.Equal(Refusal, SensitiveStatements.JsonCore("{\"a\":\"some statement text\"}", boom, ok, Refusal));
        Assert.Equal(Refusal, SensitiveStatements.JsonCore("{\"some_long_key\":1}", boom, ok, Refusal));
        Assert.Equal(Refusal, SensitiveStatements.JsonCore("plain text output", boom, ok, Refusal));
        // xml judge: a value and a whole output.
        Assert.Equal(Refusal, SensitiveStatements.JsonCore("{\"a\":\"<plan/>\"}", ok, boom, Refusal));
        Assert.Equal(Refusal, SensitiveStatements.JsonCore("<plan/>", ok, boom, Refusal));
        // a different refusal argument comes back verbatim.
        Assert.Equal("other sentence", SensitiveStatements.JsonCore("<plan/>", ok, boom, "other sentence"));
    }

    [Fact]
    public void AJudgeThatReturnsNullForWholeOutputGivesTheRefusal()
    {
        Func<string?, string?> nul = _ => null;

        Assert.Equal(Refusal, SensitiveStatements.JsonCore("plain text output", nul, nul, Refusal));
    }

    [Fact]
    public void EmptyOutputIsReturnedAsIs()
    {
        var r = new Recorder();

        Assert.Same(string.Empty, r.Run(string.Empty));
        Assert.Empty(r.TextCalls);
    }

    [Fact]
    public void CanaryPlanParsesWithThreeStatementsAndEveryNeedle()
    {
        string plan = StatementScrubCanary.CanaryPlan();

        var doc = XDocument.Parse(plan);
        Assert.Equal(3, doc.Descendants().Count(e => e.Name.LocalName == "StmtSimple"));
        foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.Contains(needle, plan);
        foreach (string needle in StatementScrubCanary.KeptNeedles) Assert.Contains(needle, plan);

        var texts = doc.Descendants().Where(e => e.Name.LocalName == "StmtSimple")
            .Select(e => (string?)e.Attribute("StatementText")).ToList();
        Assert.Equal(StatementScrubCanary.CanaryStatement, texts[0]);
        Assert.Equal(StatementScrubCanary.PlainStatement, texts[1]);
        Assert.Equal(StatementScrubCanary.AutoParamStatement, texts[2]);
        Assert.Contains("S3cret-canary-ssf", StatementScrubCanary.CanaryStatement);
        Assert.Contains("canary_plain_ssf", StatementScrubCanary.PlainStatement);
    }

    [Fact]
    public void CanaryPlanInsideJsonReachesTheXmlJudge()
    {
        var r = new Recorder();
        string plan = StatementScrubCanary.CanaryPlan();
        string output = new JsonObject { ["plan_xml"] = plan, ["note"] = "no statement here" }.ToJsonString();

        Assert.Same(output, r.Run(output));
        Assert.Equal(new[] { plan }, r.XmlCalls);
    }

    /// <summary>
    /// Measurement for the lane report, not a gate: the walk's own cost with identity judges on a 1 MB, 200-row
    /// response. It asserts only that identity judges leave the output untouched.
    /// </summary>
    [Fact]
    public void MeasureWalkCostWithIdentityJudges()
    {
        var rows = new JsonArray();
        string filler = new string('x', 5000);
        for (int i = 0; i < 200; i++)
        {
            rows.Add(new JsonObject
            {
                ["id"] = i,
                ["query_text"] = "select " + filler + " from dbo.t where c = " + i,
                ["query_plan"] = "<ShowPlanXML>" + filler.Substring(0, 100) + "</ShowPlanXML>",
                ["database_name"] = "db" + i,
            });
        }
        string output = new JsonObject { ["rows"] = rows }.ToJsonString();
        Assert.InRange(Encoding.UTF8.GetByteCount(output), 1_000_000, 1_300_000);

        Func<string?, string?> identity = s => s;
        Assert.Same(output, SensitiveStatements.JsonCore(output, identity, identity, Refusal));

        var times = new List<double>();
        for (int run = 0; run < 20; run++)
        {
            long start = Stopwatch.GetTimestamp();
            string result = SensitiveStatements.JsonCore(output, identity, identity, Refusal);
            times.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
            Assert.Same(output, result);
        }
        times.Sort();
        double median = (times[9] + times[10]) / 2;
        string line = $"JsonCore identity walk, {Encoding.UTF8.GetByteCount(output)} bytes, 200 rows, 20 runs: median {median:F2} ms, min {times[0]:F2} ms, max {times[19]:F2} ms";
        _output.WriteLine(line);
    }
}
