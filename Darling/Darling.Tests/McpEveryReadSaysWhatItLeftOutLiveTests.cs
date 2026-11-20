/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: reads only the scratch database McpEveryReadStore mints. */
/// <summary>
/// A reply that leaves rows out says so. This runs two rules across every read the MCP server lists (<see cref="McpEveryReadStore"/>),
/// over a seed whose rows carry long text:
/// <list type="bullet">
/// <item>a reply fits <see cref="McpResponseBudget.DefaultBytes"/>, which a read holds to by cutting its rows and saying it did;</item>
/// <item>when a read is asked for one row through a row cap its schema carries (<see cref="McpEveryReadStore.RowCapParameters"/>) and
/// answers with fewer rows than it did without the cap, the capped reply says rows were left out: a flag whose name holds
/// <c>truncated</c> is true, <c>has_more</c> is true, <c>next_offset</c> is set, or a <c>total</c> count is more than the rows returned.</item>
/// </list>
///
/// <para>No read is left out by name. A read that does not answer data here is reported with the reason, taken from its own answer, and
/// <see cref="MeasuredShare"/> holds a floor under the share of the listed reads that were measured.</para>
/// </summary>
[Collection("gap-cache-serial")]
public sealed class McpEveryReadSaysWhatItLeftOutLiveTests : IClassFixture<McpEveryReadStore>
{
    /// <summary>The least share of the listed reads that answer data and are measured.</summary>
    private const double MeasuredShare = 0.6;

    private readonly McpEveryReadStore _store;

    public McpEveryReadSaysWhatItLeftOutLiveTests(McpEveryReadStore store) => _store = store;

    [Fact]
    public async Task EveryReply_FitsTheResponseBudget_AndACappedReply_SaysRowsWereLeftOut_OnEveryRead()
    {
        Assert.SkipWhen(!_store.Available, McpEveryReadStore.SkipReason);

        var broken = new List<string>();
        var notMeasured = new List<string>();
        var measured = 0;
        var capped = 0;

        foreach (var tool in _store.Tools)
        {
            var (arguments, answer) = await _store.WideReadAsync(tool);
            if (answer.NotDataReason is { } reason)
            {
                notMeasured.Add($"{tool.Name}: {reason}");
                continue;
            }

            measured++;
            var bytes = Encoding.UTF8.GetByteCount(answer.Text);
            if (bytes > McpResponseBudget.DefaultBytes)
            {
                broken.Add($"{tool.Name}: the reply is {bytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte response budget [{McpEveryReadStore.Describe(arguments)}]");
            }

            var (path, rows) = LargestArray(answer.Text);
            if (rows < 2)
            {
                continue;
            }

            foreach (var cap in McpEveryReadStore.RowCapParameters.Where(tool.Takes))
            {
                var (cappedArguments, cappedAnswer) = await _store.WideReadAsync(tool, new Dictionary<string, object?> { [cap] = 1 });
                if (cappedAnswer.NotDataReason is not null)
                {
                    continue;
                }

                using var document = JsonDocument.Parse(cappedAnswer.Text);
                var kept = CountAt(document.RootElement, path);
                if (kept is not int count || count >= rows)
                {
                    continue;
                }

                capped++;
                if (!SaysRowsWereLeftOut(document.RootElement, count))
                {
                    broken.Add($"{tool.Name}: {cap} 1 returns {count} of the {rows} rows at {path} and no field says rows were left out [{McpEveryReadStore.Describe(cappedArguments)}]");
                }
            }
        }

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"{_store.Tools.Count} tools listed, {measured} answered data and were measured, {capped} capped answers left rows out."
            + Environment.NewLine + "Not measured:" + Environment.NewLine + string.Join(Environment.NewLine, notMeasured));

        Assert.True(broken.Count == 0,
            $"{broken.Count} finding(s): a reply is over the response budget, or leaves rows out and does not say so:"
            + Environment.NewLine + string.Join(Environment.NewLine, broken));
        Assert.True(measured >= _store.Tools.Count * MeasuredShare,
            $"Only {measured} of the {_store.Tools.Count} listed reads answered data, under the floor of {MeasuredShare:P0}. Not measured:"
            + Environment.NewLine + string.Join(Environment.NewLine, notMeasured));
    }

    /// <summary>
    /// The two ranked query reads cut a page two ways, and each cut has its own name. A page cut by <c>top</c> alone says
    /// <c>truncated</c> and does not say the budget cut it: <c>budget_truncated</c> is false and <c>budget_note</c> is null. The
    /// reading guide the server gives for the read names both keys, so a caller who reads it knows which cut a flag means.
    /// </summary>
    [Theory]
    [InlineData("get_top_queries_by_cpu")]
    [InlineData("get_query_store_top")]
    public async Task APageCutByTopAlone_SaysTruncated_AndNotBudgetTruncated_AndTheGuideNamesBothCuts(string name)
    {
        Assert.SkipWhen(!_store.Available, McpEveryReadStore.SkipReason);

        var tool = _store.Tools.Single(t => t.Name == name);
        var (wideArguments, wide) = await _store.WideReadAsync(tool);
        Assert.True(wide.NotDataReason is null, $"{name} did not answer data: {wide.NotDataReason} [{McpEveryReadStore.Describe(wideArguments)}]");
        using (var wideDocument = JsonDocument.Parse(wide.Text))
        {
            Assert.True(wideDocument.RootElement.GetProperty("queries").GetArrayLength() >= 2,
                $"{name} needs two queries in the window for top 1 to leave one out [{McpEveryReadStore.Describe(wideArguments)}]");
        }

        var (arguments, capped) = await _store.WideReadAsync(tool, new Dictionary<string, object?> { ["top"] = 1 });
        Assert.True(capped.NotDataReason is null, $"{name} did not answer data: {capped.NotDataReason} [{McpEveryReadStore.Describe(arguments)}]");
        using var document = JsonDocument.Parse(capped.Text);
        var root = document.RootElement;
        Assert.Equal(1, root.GetProperty("queries").GetArrayLength());
        Assert.True(root.GetProperty("truncated").GetBoolean(), $"{name} top 1 left queries out and truncated is false");
        Assert.True(root.TryGetProperty("budget_truncated", out var budgetCut) && budgetCut.ValueKind == JsonValueKind.False,
            $"{name} top 1 fits the response budget, so budget_truncated is false: {capped.Text[..Math.Min(capped.Text.Length, 300)]}");
        Assert.True(root.TryGetProperty("budget_note", out var budgetNote) && budgetNote.ValueKind == JsonValueKind.Null,
            $"{name} top 1 fits the response budget, so budget_note is null");

        var guide = await _store.CallAsync("get_tool_guide", new Dictionary<string, object?> { ["tools"] = new[] { name } });
        Assert.False(guide.IsError, $"get_tool_guide answered an error for {name}: {guide.Text[..Math.Min(guide.Text.Length, 300)]}");
        Assert.Contains("by the top cap or by the budget", guide.Text, StringComparison.Ordinal);
        Assert.Contains("budget_truncated", guide.Text, StringComparison.Ordinal);
    }

    /// <summary>The path and length of the reply's longest array of objects: the rows a cap would cut.</summary>
    private static (string Path, int Rows) LargestArray(string reply)
    {
        try
        {
            using var document = JsonDocument.Parse(reply);
            var best = (Path: "$", Rows: 0);
            Walk(document.RootElement, "$", ref best);
            return best;
        }
        catch (JsonException)
        {
            return ("$", 0);
        }
    }

    private static void Walk(JsonElement element, string path, ref (string Path, int Rows) best)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    Walk(property.Value, path + "." + property.Name, ref best);
                }

                break;
            case JsonValueKind.Array:
                var length = element.GetArrayLength();
                if (length > best.Rows && element[0].ValueKind == JsonValueKind.Object)
                {
                    best = (path, length);
                }

                break;
        }
    }

    /// <summary>The length of the array at <paramref name="path"/> (property names from the root), or null when the reply has none there.</summary>
    private static int? CountAt(JsonElement root, string path)
    {
        var element = root;
        foreach (var name in path.Split('.').Skip(1))
        {
            if (element.ValueKind != JsonValueKind.Object || !element.TryGetProperty(name, out element))
            {
                return null;
            }
        }

        return element.ValueKind == JsonValueKind.Array ? element.GetArrayLength() : null;
    }

    /// <summary>Whether any field of the reply, at any depth outside its rows, says rows were left out.</summary>
    private static bool SaysRowsWereLeftOut(JsonElement element, int returned)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        foreach (var property in element.EnumerateObject())
        {
            var name = property.Name;
            var value = property.Value;
            if (value.ValueKind == JsonValueKind.True && (name.Contains("truncated", StringComparison.Ordinal) || name == "has_more")) return true;
            if (value.ValueKind == JsonValueKind.Number && name == "next_offset") return true;
            if (value.ValueKind == JsonValueKind.Number && name.Contains("total", StringComparison.Ordinal)
                && value.TryGetInt64(out var total) && total > returned) return true;
            if (SaysRowsWereLeftOut(value, returned)) return true;
        }

        return false;
    }
}
