/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348: the web side of the statement filter. Every <c>/api/read/*</c> route answers through
/// <c>DarlingWebEndpoints.ToHttpResult</c>, the mute-rule routes through <c>MuteRuleToolResult</c>, and triage, the
/// alert notebook and the fleet sweep feed write their own body through <c>DarlingWebStatementSweep.JsonText</c>. Each
/// case hands the writer a tool answer that still holds the canary (the raw answer: the MCP host's filter is not in the
/// web path) and reads back what a browser would get. The tests are RED on a writer with no sweep, because the canary
/// then comes through.
/// </summary>
public sealed class StatementFilterWebTests
{
    private static readonly string Canary = StatementScrubCanary.CanaryStatement;
    private static readonly string Plain = StatementScrubCanary.PlainStatement;

    private static async Task<(int Status, string Body)> RunAsync(IResult result)
    {
        var context = new DefaultHttpContext
        {
            RequestServices = new ServiceCollection().AddLogging().BuildServiceProvider(),
        };
        context.Response.Body = new MemoryStream();
        await result.ExecuteAsync(context);
        context.Response.Body.Position = 0;
        return (context.Response.StatusCode, Encoding.UTF8.GetString(((MemoryStream)context.Response.Body).ToArray()));
    }

    private static Task<(int Status, string Body)> ReadRoute(string tool, string raw) =>
        RunAsync(DarlingWebEndpoints.ToHttpResult(raw, "/api/read/" + tool, NullLogger.Instance, 1));

    private static void AssertWithheld(string body)
    {
        foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, body);
        Assert.Contains(StatementFilterCensus.Marker, body);
    }

    [Fact]
    public async Task GetActiveQueries_WithholdsANamedStatement_AndKeepsAPlainOne()
    {
        string raw = new JsonObject
        {
            ["server"] = "srv-a",
            ["queries"] = new JsonArray(
                new JsonObject { ["session_id"] = 55, ["query_text"] = Canary },
                new JsonObject { ["session_id"] = 56, ["query_text"] = Plain }),
        }.ToJsonString();
        StatementFilterCensus.AssertRawHoldsTheCanary(raw);

        var (status, body) = await ReadRoute("get_active_queries", raw);

        Assert.Equal(200, status);
        AssertWithheld(body);
        Assert.Contains(Plain, body);
        Assert.Equal(55, JsonNode.Parse(body)!["queries"]![0]!["session_id"]!.GetValue<int>());
    }

    [Fact]
    public async Task GetPlanXml_WrappedJson_WithholdsStatementOne_AndKeepsStatementTwo()
    {
        string plan = StatementScrubCanary.CanaryPlan();
        string raw = DarlingWebEndpoints.WrapPlanXml(plan, "0xABCD", "Orders");
        StatementFilterCensus.AssertRawHoldsTheCanary(raw);

        var (status, body) = await ReadRoute("get_plan_xml", raw);

        Assert.Equal(200, status);
        string filteredPlan = JsonNode.Parse(body)!["plan_xml"]!.GetValue<string>();
        StatementFilterCensus.AssertPlanFilteredKeepsTheRest(plan, filteredPlan);
        Assert.Equal("0xABCD", JsonNode.Parse(body)!["query_hash"]!.GetValue<string>());
    }

    [Fact]
    public async Task GetDeadlockDetail_CardsAreWithheld_EvenWhenTheStatementIsCutAtTheCap()
    {
        // A statement that is named only AFTER the card's cap: the card holds an innocent-looking prefix unless the
        // text is judged whole before it is cut.
        string longCanary = "/* " + new string('x', DarlingWebDeadlockGraph.SqlTextCap) + " */ " + Canary;
        string graph = "<deadlock><victim-list><victimProcess id=\"p1\"/></victim-list><process-list>"
            + "<process id=\"p1\" spid=\"55\" waittime=\"100\" lockMode=\"X\"><executionStack/><inputbuf>" + longCanary + "</inputbuf></process>"
            + "<process id=\"p2\" spid=\"56\" waittime=\"200\" lockMode=\"S\"><executionStack/><inputbuf>" + Plain + "</inputbuf></process>"
            + "</process-list><resource-list>"
            + "<keylock hobtid=\"1\" dbid=\"5\" objectname=\"db.dbo.t\" indexname=\"pk\" id=\"k1\" mode=\"X\"><owner-list><owner id=\"p2\" mode=\"X\"/></owner-list><waiter-list><waiter id=\"p1\" mode=\"S\" requestType=\"wait\"/></waiter-list></keylock>"
            + "<keylock hobtid=\"2\" dbid=\"5\" objectname=\"db.dbo.u\" indexname=\"pk\" id=\"k2\" mode=\"X\"><owner-list><owner id=\"p1\" mode=\"X\"/></owner-list><waiter-list><waiter id=\"p2\" mode=\"S\" requestType=\"wait\"/></waiter-list></keylock>"
            + "</resource-list></deadlock>";
        string tool = new JsonObject
        {
            ["deadlocks"] = new JsonArray(new JsonObject { ["deadlock_graph_xml"] = graph }),
        }.ToJsonString();

        string withCards = DarlingWebDeadlockGraph.AddGraphs(tool);
        Assert.Contains("\"graph\"", withCards);

        var (status, body) = await ReadRoute("get_deadlock_detail", withCards);

        Assert.Equal(200, status);
        AssertWithheld(body);
        Assert.Contains(Plain, body);
        var processes = JsonNode.Parse(body)!["deadlocks"]![0]!["graph"]!["processes"]!.AsArray();
        Assert.Equal(StatementFilterCensus.Marker, processes[0]!["sql_text"]!.GetValue<string>());
    }

    [Fact]
    public async Task AlertsTest_Answer_IsSwept()
    {
        string raw = new JsonObject
        {
            ["status"] = "ok",
            ["sample_rows"] = new JsonArray(new JsonObject { ["query_text"] = Canary, ["wait_type"] = "LCK_M_X" }),
        }.ToJsonString();

        var (_, body) = await RunAsync(DarlingWebEndpoints.ToHttpResult(raw, "/api/alerts/test", NullLogger.Instance, 1));

        AssertWithheld(body);
        Assert.Contains("LCK_M_X", body);
    }

    [Fact]
    public async Task MuteRuleEnabled_EchoOfACanaryPattern_IsWithheld_AndKeepsItsStatus()
    {
        string raw = new JsonObject
        {
            ["status"] = "ok",
            ["rule"] = new JsonObject { ["id"] = "r1", ["enabled"] = true, ["pattern"] = Canary },
        }.ToJsonString();

        var (status, body) = await RunAsync(DarlingWebEndpoints.MuteRuleToolResult(
            raw, "/api/mute-rules/{id}/enabled", NullLogger.Instance, 1));

        Assert.Equal(200, status);
        AssertWithheld(body);
        Assert.True(JsonNode.Parse(body)!["rule"]!["enabled"]!.GetValue<bool>());
    }

    [Fact]
    public async Task AnAnswerThatNamesNothing_IsWrittenByteForByte()
    {
        string raw = "{\"rows\":[{\"query_text\":\"" + Plain + "\",\"n\":3}]}";

        var (status, body) = await ReadRoute("get_active_queries", raw);

        Assert.Equal(200, status);
        Assert.Equal(raw, body);
    }

    [Fact]
    public async Task AnAnswerTheSweepCannotRead_IsNeverWritten_ItBecomesTheRefusalSentence()
    {
        // Opens like JSON and does not parse: the sweep refuses it instead of judging its escaped text.
        string raw = "{\"query_text\":\"" + Canary + "\"";

        var (status, body) = await ReadRoute("get_active_queries", raw);

        Assert.Equal(500, status);
        Assert.Contains(SensitiveStatements.JsonRefusal, body);
        foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, body);
    }

    [Fact]
    public async Task ASlowReadRow_WithACanaryInItsArguments_IsWithheldOnTheWebRead()
    {
        string raw = new JsonObject
        {
            ["slow_reads"] = new JsonArray(new JsonObject
            {
                ["route"] = "analyze_plan_xml",
                ["arguments"] = new JsonObject { ["statement"] = Canary }.ToJsonString(),
            }),
        }.ToJsonString();

        var (_, body) = await ReadRoute("get_slow_reads", raw);

        AssertWithheld(body);
        Assert.Contains("analyze_plan_xml", body);
    }

    [Theory]
    [InlineData("/api/triage")]
    [InlineData("/api/alert-notebook")]
    [InlineData("/api/sweeps")]
    public async Task AnEndpointsOwnBody_IsSweptBeforeItIsWritten(string route)
    {
        var body = new JsonObject
        {
            ["alert"] = new JsonObject { ["detail"] = Canary, ["metric"] = "blocking" },
            ["sections"] = new JsonArray(new JsonObject { ["rows"] = new JsonArray(new JsonObject { ["query_text"] = Plain }) }),
        };

        var (status, written) = await RunAsync(DarlingWebStatementSweep.JsonText(body, route, NullLogger.Instance, 1));

        Assert.Equal(200, status);
        AssertWithheld(written);
        Assert.Contains(Plain, written);
        Assert.Equal("blocking", JsonNode.Parse(written)!["alert"]!["metric"]!.GetValue<string>());
    }

    [Fact]
    public void SlowReadArguments_AreJudgedBeforeTheyAreStored()
    {
        var named = new JsonObject { ["statement"] = Canary, ["hours"] = 4 };
        string swept = SensitiveStatements.Json(named.ToJsonString());

        var stored = SlowReadLog.ArgumentsAfterSweep(named, swept)!;

        foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, stored.ToJsonString());
        Assert.Contains(StatementFilterCensus.Marker, stored.ToJsonString());
        Assert.Equal(4, stored["hours"]!.GetValue<int>());

        var plain = new JsonObject { ["statement"] = Plain };
        Assert.Same(plain, SlowReadLog.ArgumentsAfterSweep(plain, SensitiveStatements.Json(plain.ToJsonString())));

        var refused = SlowReadLog.ArgumentsAfterSweep(plain, SensitiveStatements.JsonRefusal)!;
        Assert.DoesNotContain(Plain, refused.ToJsonString());
        Assert.Same(plain, SlowReadLog.ArgumentsAfterSweep(plain, null));
    }

    /// <summary>
    /// The page's plan panel draws what the read route answered. The answer is the real one (the wrapped canary plan
    /// through <c>ToHttpResult</c>); the shipped <c>plan-viewer.js</c> runs on it under Node, and the text it shows is
    /// checked: statement 1 (header and parameter values) shows the marker, statement 2's parameters are intact.
    /// </summary>
    [Fact]
    public async Task ThePlanPanel_ShowsStatementOneAsTheMarker_AndStatementTwoIntact()
    {
        var (_, answered) = await ReadRoute("get_plan_xml", DarlingWebEndpoints.WrapPlanXml(StatementScrubCanary.CanaryPlan(), "0xABCD", "Orders"));
        string file = Path.Combine(Path.GetTempPath(), "ssf-l7-plan-answer-" + Guid.NewGuid().ToString("N") + ".json");
        File.WriteAllText(file, answered);
        try
        {
            var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
            psi.ArgumentList.Add(RepoFile.PathTo("Darling", "Darling.Tests", "web-plan-viewer-harness.mjs"));
            psi.ArgumentList.Add(RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
            psi.ArgumentList.Add("answeredPlan");
            psi.ArgumentList.Add(file);

            Process? started = null;
            try
            {
                started = Process.Start(psi);
            }
            catch (Win32Exception)
            {
                Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            }

            /* Assert.Skip throws, so a missing Node never reaches here; the null is only a Start that returned no process. */
            using var proc = started ?? throw new InvalidOperationException("Starting the plan viewer harness returned no process.");
            {
                var error = proc.StandardError.ReadToEndAsync();
                string output = proc.StandardOutput.ReadToEnd().Trim();
                Assert.True(proc.WaitForExit(20000), "the plan viewer harness did not finish in 20 s");
                Assert.True(proc.ExitCode == 0, "the plan viewer harness failed: " + await error);
                using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
                string shown = doc.RootElement.GetProperty("pre").GetString()!;

                foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, shown);
                Assert.Contains("StatementText=\"" + StatementFilterCensus.Marker + "\"", shown);
                Assert.Contains(StatementScrubCanary.PlainStatement, shown);
                Assert.Contains("ParameterCompiledValue=\"N'param-canary-ssf'\"", shown);
                Assert.Contains("ssf_autoparam_canary", shown);
            }
        }
        finally
        {
            File.Delete(file);
        }
    }

    /// <summary>The sweep's cost on a read that names nothing: a 1 MB answer of plain statements and numbers. The
    /// bound is loose on purpose (the sweep's own budget is 1.5 s); the median is printed for the record.</summary>
    [Fact]
    public async Task AOneMegabyteAnswerThatNamesNothing_IsWrittenUnchanged_AndSweptInsideTheBudget()
    {
        var rows = new JsonArray();
        for (int i = 0; rows.Count < 12000; i++)
        {
            rows.Add(new JsonObject { ["id"] = i, ["query_text"] = Plain + " /* " + i + " */", ["wait_type"] = "PAGEIOLATCH_SH", ["ms"] = i * 3.5 });
        }

        string raw = new JsonObject { ["rows"] = rows }.ToJsonString();
        Assert.InRange(raw.Length, 1_000_000, 2_000_000);

        var times = new List<double>();
        for (int run = 0; run < 7; run++)
        {
            var clock = System.Diagnostics.Stopwatch.StartNew();
            var (status, body) = await ReadRoute("get_active_queries", raw);
            times.Add(clock.Elapsed.TotalMilliseconds);
            Assert.Equal(200, status);
            Assert.Equal(raw.Length, body.Length);
        }

        times.Sort();
        Console.WriteLine("web sweep, 1 MB no-hit response: median " + times[times.Count / 2].ToString("F0", System.Globalization.CultureInfo.InvariantCulture) + " ms");
        Assert.True(times[times.Count / 2] < 1500, "median " + times[times.Count / 2] + " ms");
    }
}
