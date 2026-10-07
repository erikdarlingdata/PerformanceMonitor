/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348: the statement filter as the Darling host REGISTERS it. <see cref="SensitiveStatementOutputFilterTests"/>
/// runs the filter alone around a fake tool; these run the host's real filter list (the unknown-argument guard, the
/// latency filter, GCF, the statement filter) through a real server, with and without
/// <c>DARLING_OUTPUT_FORMAT=gcf</c>, so the slot the filter holds in that list is part of what is pinned: placed
/// before GCF it would read a GCF wire instead of the tool's JSON, and the canary would come back.
/// </summary>
[Collection("DarlingOutputFormatEnv")]
public sealed class SensitiveStatementHostFilterTests : IDisposable
{
    private const string Marker = SensitiveStatements.PlaceholderText;

    public SensitiveStatementHostFilterTests() =>
        Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", null);

    public void Dispose() => Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", null);

    /// <summary>Eight uniform rows, so a GCF wire is smaller than the JSON and GCF really re-encodes the answer.
    /// Row 1 carries the canary, row 2 a canary with a line feed between CREATE and LOGIN (a GCF wire escapes the
    /// line feed, which defeats the filter's token match), the rest are plain.</summary>
    private static string CanaryRows()
    {
        var rows = new JsonArray();
        for (int i = 0; i < 8; i++)
        {
            string text = i switch
            {
                1 => StatementScrubCanary.CanaryStatement,
                2 => "CREATE\nLOGIN [canary_split_ssf] WITH PASSWORD = N'S3cret-canary-ssf'",
                _ => StatementScrubCanary.PlainStatement,
            };
            rows.Add(new JsonObject { ["query_text"] = text, ["executions"] = 100 + i, ["avg_ms"] = 10 + i });
        }

        return new JsonObject { ["rows"] = rows }.ToJsonString();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheHostsRealFilterList_WithholdsTheCanaryPrecisely_WithAndWithoutGcf(bool gcf)
    {
        Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", gcf ? "gcf" : null);
        // gcf: null = the production wiring, the environment variable (#5320); the census default pins plain output
        using var server = await StatementFilterCensus.BuildHostAsync(gcf: null);
        string raw = CanaryRows();
        StatementFilterCensus.AssertRawHoldsTheCanary(raw);
        if (gcf)
        {
            // the precondition: with nothing swept, GCF really re-encodes this answer, so the order is under test
            Assert.NotNull(GcfOutput.TryEncode(raw));
        }

        var (text, isError) = await StatementFilterCensus.FilterThroughHostAsync(server, raw);

        Assert.False(isError);
        StatementFilterCensus.AssertFilteredWithholdsTheCanary(raw, text);
        Assert.DoesNotContain("canary_ssf", text);
        Assert.DoesNotContain("canary_split_ssf", text);
        Assert.Equal(gcf, !LooksLikeJson(text));

        // precise: the two canary rows are the marker and the six plain rows are untouched, in either encoding
        Assert.Equal(2, Count(text, Marker));
        Assert.Equal(6, Count(text, "canary_plain_ssf"));
        if (!gcf)
        {
            var rows = JsonNode.Parse(text)!["rows"]!.AsArray();
            Assert.Equal(8, rows.Count);
            Assert.Equal(Marker, (string?)rows[1]!["query_text"]);
            Assert.Equal(Marker, (string?)rows[2]!["query_text"]);
            foreach (int i in new[] { 0, 3, 4, 5, 6, 7 })
            {
                Assert.Equal(StatementScrubCanary.PlainStatement, (string?)rows[i]!["query_text"]);
            }
        }
    }

    /// <summary>#5320: the census host's format is pinned, so the environment variable (which another class in this
    /// collection, or a live class running beside it, may have set) cannot change what a census answer looks like. Pinned
    /// plain output returns a page with no hit byte for byte, while the variable says gcf.</summary>
    [Fact]
    public async Task APinnedPlainHost_ReturnsAPageWithNoHitUnchanged_EvenWhileTheEnvironmentSaysGcf()
    {
        Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", "gcf");
        using var server = await StatementFilterCensus.BuildHostAsync(gcf: false);
        string clean = CleanRows();
        Assert.NotNull(GcfOutput.TryEncode(clean)); // the precondition: an unpinned GCF host WOULD re-encode this page

        var (text, isError) = await StatementFilterCensus.FilterThroughHostAsync(server, clean);

        Assert.False(isError);
        Assert.Equal(clean, text);
    }

    /// <summary>#5320: the other pin. GCF pinned ON re-encodes the answer whatever the variable says, the filter still
    /// reads the tool's own JSON first (it sits next to the tool), and so a hit is still the marker after GCF encoding.</summary>
    [Fact]
    public async Task APinnedGcfHost_StillWithholdsTheCanary_AfterGcfEncoding_WhileTheEnvironmentIsUnset()
    {
        Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", null);
        using var server = await StatementFilterCensus.BuildHostAsync(gcf: true);
        string raw = CanaryRows();
        StatementFilterCensus.AssertRawHoldsTheCanary(raw);
        Assert.NotNull(GcfOutput.TryEncode(raw));

        var (text, isError) = await StatementFilterCensus.FilterThroughHostAsync(server, raw);

        Assert.False(isError);
        Assert.False(LooksLikeJson(text), "the pinned-on host did not GCF-encode the answer");
        StatementFilterCensus.AssertFilteredWithholdsTheCanary(raw, text);
        Assert.DoesNotContain("canary_ssf", text);
        Assert.DoesNotContain("canary_split_ssf", text);
        Assert.Equal(2, Count(text, Marker));
        Assert.Equal(6, Count(text, "canary_plain_ssf"));
    }

    /// <summary>Eight uniform plain rows: nothing for the filter to withhold, and big enough for GCF to be smaller.</summary>
    private static string CleanRows()
    {
        var rows = new JsonArray();
        for (int i = 0; i < 8; i++)
        {
            rows.Add(new JsonObject
            {
                ["id"] = i,
                ["database_name"] = "db_" + i,
                ["query_text"] = StatementScrubCanary.PlainStatement,
                ["duration_ms"] = 100 + i,
            });
        }

        return new JsonObject { ["rows"] = rows }.ToJsonString();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnErrorResultThroughTheHostsRealFilterList_IsSwept_AndStaysAnError(bool gcf)
    {
        Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", gcf ? "gcf" : null);
        // gcf: null = the production wiring, the environment variable (#5320); the census default pins plain output
        using var server = await StatementFilterCensus.BuildHostAsync(gcf: null);
        string raw = new JsonObject
        {
            ["status"] = "error",
            ["message"] = StatementScrubCanary.CanaryStatement,
        }.ToJsonString();

        var (text, isError) = await StatementFilterCensus.FilterThroughHostAsync(server, raw, isError: true);

        Assert.True(isError);
        StatementFilterCensus.AssertFilteredWithholdsTheCanary(raw, text);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnMcpExceptionThatNamesAStatement_ThroughTheHostsRealFilterList_ReachesTheClientSwept(bool gcf)
    {
        // L1: the SDK builds an error result from a thrown McpException's message OUTSIDE the result sweep, so the
        // filter has to catch the exception itself. Without that catch the client reads the canary in the message.
        Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", gcf ? "gcf" : null);
        using var server = await StatementFilterCensus.BuildHostAsync();
        string message = StatementScrubCanary.CanaryStatement;

        var (text, isError) = await StatementFilterCensus.CallRealToolAsync(
            server, StatementFilterCensus.ThrowProbeName, new JsonObject { ["message"] = message });

        Assert.True(isError);
        StatementFilterCensus.AssertFilteredWithholdsTheCanary(message, text);
    }

    [Fact]
    public async Task AnMcpExceptionThatNamesNoStatement_ThroughTheHostsRealFilterList_KeepsItsMessage()
    {
        using var server = await StatementFilterCensus.BuildHostAsync();

        var (text, isError) = await StatementFilterCensus.CallRealToolAsync(
            server, StatementFilterCensus.ThrowProbeName, new JsonObject { ["message"] = "server_name is required" });

        Assert.True(isError);
        Assert.Contains("server_name is required", text);
        Assert.DoesNotContain(Marker, text);
    }

    [Fact]
    public async Task AnalyzePlanXmlOnTheCanaryPlan_ThroughTheHost_WithholdsStatementOneAndKeepsStatementTwo()
    {
        using var server = await StatementFilterCensus.BuildHostAsync();
        string plan = StatementScrubCanary.CanaryPlan();

        var (text, isError) = await StatementFilterCensus.CallRealToolAsync(
            server, "analyze_plan_xml", new JsonObject { ["plan_xml"] = plan });

        Assert.False(isError);
        StatementFilterCensus.AssertFilteredWithholdsTheCanary(plan, text);
        JsonNode.Parse(text);
    }

    [Fact]
    public async Task TheFilterAlsoCoversTheCoreEndpoint_AndTheToolsAddedSincePlanning()
    {
        using var server = await StatementFilterCensus.BuildHostAsync();

        // The filter list is global to the host, so a tool added after the plan is swept by the same registration.
        // Pin that the newer tool is served at all, so the claim is about a tool this host really dispatches.
        Assert.Contains("get_query_store_query_history", await StatementFilterCensus.ListToolNamesAsync(server));

        var (text, isError) = await StatementFilterCensus.CallRealToolAsync(
            server, "get_tool_guide", new JsonObject(), "/core");
        Assert.False(isError);
        JsonNode.Parse(text);
    }

    [Fact]
    public async Task EveryGetToolGuideTopic_IsByteIdenticalThroughTheSweep_AndThroughTheHost()
    {
        using var server = await StatementFilterCensus.BuildHostAsync();
        var catalog = server.Services.GetRequiredService<McpToolGuideCatalog>();
        var topics = McpToolGuideTopics.All;
        Assert.NotEmpty(topics);

        foreach (string? name in topics.Select(t => t.Name).Prepend(null))
        {
            string[]? names = name is null ? null : new[] { name };
            string rendered = McpToolGuide.Render(catalog, topics, null, names);

            // through the sweep alone: the very same text, not an equal copy
            var swept = SensitiveStatementOutputFilter.Sweep(new ModelContextProtocol.Protocol.CallToolResult
            {
                Content = new System.Collections.Generic.List<ModelContextProtocol.Protocol.ContentBlock> { new ModelContextProtocol.Protocol.TextContentBlock { Text = rendered } },
            });
            Assert.Same(rendered, ((ModelContextProtocol.Protocol.TextContentBlock)swept.Content[0]).Text);

            // through the host's whole registered list
            var args = new JsonObject();
            if (names is not null) args["topics"] = new JsonArray(JsonValue.Create(name));
            var (text, isError) = await StatementFilterCensus.CallRealToolAsync(server, "get_tool_guide", args);
            Assert.False(isError);
            Assert.Equal(rendered, text);
        }
    }

    [Fact]
    public void TheHostRegistersTheStatementFilterLast_AfterGcf()
    {
        string source = File.ReadAllText(HostSourcePath());
        int gcf = source.IndexOf("AddCallToolFilter(GcfCallToolFilter.Instance)", StringComparison.Ordinal);
        int sweep = source.IndexOf("AddCallToolFilter(SensitiveStatementOutputFilter.Instance)", StringComparison.Ordinal);

        Assert.True(gcf > 0, "the host no longer registers GCF through AddCallToolFilter(GcfCallToolFilter.Instance)");
        Assert.True(sweep > gcf, "the statement filter must be registered AFTER GCF: the last-added filter sits next to the tool");
        Assert.Equal(sweep, source.LastIndexOf(".AddCallToolFilter(", StringComparison.Ordinal) + 1);
        Assert.Equal(2, source.Split("AddCallToolFilter(SensitiveStatementOutputFilter.Instance)").Length);
    }

    private static bool LooksLikeJson(string text)
    {
        try
        {
            JsonDocument.Parse(text).Dispose();
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static int Count(string text, string needle)
    {
        int count = 0;
        for (int at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0;
             at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private static string HostSourcePath()
    {
        string? dir = AppContext.BaseDirectory;
        while (dir is not null && !Directory.Exists(Path.Combine(dir, "Darling", "PerformanceMonitor.Darling.Service")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        if (dir is null) throw new InvalidOperationException("The repository root was not found above the test binaries.");
        return Path.Combine(dir, "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs");
    }
}
