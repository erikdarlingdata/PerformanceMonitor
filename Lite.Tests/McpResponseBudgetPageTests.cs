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
using System.Text.Json.Nodes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6131: the shared page-cut helper behind <c>get_query_store_top</c>'s response budget, and the shared
/// <c>get_client_timeouts</c> payload that returned the same monitoring-tool T-SQL twice. Both live in code the Lite and Darling
/// tools share, so these run over the real builders, no database.
/// </summary>
public sealed class McpResponseBudgetPageTests
{
    private static JsonObject Payload(int rows, int textLength)
    {
        var array = new JsonArray();
        for (var i = 0; i < rows; i++)
        {
            array.Add(new JsonObject { ["id"] = i, ["text"] = new string('x', textLength) });
        }

        return new JsonObject { ["server"] = "s", ["window_truncated"] = false, ["queries"] = array };
    }

    [Fact]
    public void CapPage_CutsAWidePageUnderTheBudget_KeepsTheLeadingRows_AndSaysSo()
    {
        var json = McpResponseBudget.CapPage(Payload(200, 900), "queries", "heaviest first", "queries");

        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        var kept = root.GetProperty("queries").GetArrayLength();
        Assert.InRange(kept, 1, 199);
        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Contains($"only {kept}", root.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
        Assert.Contains("response budget", root.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
        /* The rows kept are the first ones, in order. */
        for (var i = 0; i < kept; i++)
        {
            Assert.Equal(i, root.GetProperty("queries")[i].GetProperty("id").GetInt32());
        }

        /* It is the LARGEST page that fits: one more row would not. */
        var oneMore = McpResponseBudget.CapPage(Payload(kept + 1, 900), "queries", "heaviest first", "queries", int.MaxValue);
        Assert.True(Encoding.UTF8.GetByteCount(oneMore) > McpResponseBudget.DefaultBytes - 300);
    }

    [Fact]
    public void CapPage_LeavesAPageThatFitsAlone_WithTruncatedFalse()
    {
        var json = McpResponseBudget.CapPage(Payload(5, 40), "queries", "heaviest first", "queries");

        using var doc = JsonDocument.Parse(json);
        Assert.Equal(5, doc.RootElement.GetProperty("queries").GetArrayLength());
        Assert.False(doc.RootElement.GetProperty("truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("budget_note").ValueKind);
    }

    [Fact]
    public void CapPage_KeepsOneRow_EvenWhenThatRowAloneIsOverTheBudget()
    {
        var json = McpResponseBudget.CapPage(Payload(3, McpResponseBudget.DefaultBytes * 2), "queries", "heaviest first", "queries");

        using var doc = JsonDocument.Parse(json);
        Assert.Equal(1, doc.RootElement.GetProperty("queries").GetArrayLength());
        Assert.True(doc.RootElement.GetProperty("truncated").GetBoolean());
    }

    private static AttentionWindow WindowWithTopStatement(string name, string text) =>
        new(
            new[] { DateTime.UtcNow },
            Array.Empty<AttentionCountRow>(),
            Array.Empty<AttentionSampleGroup>(),
            new[] { new AttentionTopItem(name, 3, text) },
            Array.Empty<AttentionTopItem>(),
            Array.Empty<AttentionTopItem>());

    private static ClientTimeoutSample Sample(string fingerprint, string? text, int session) =>
        new(DateTime.UtcNow, DateTime.UtcNow, "sales", "monitor", "host", session, 30_000_000, fingerprint, text);

    [Fact]
    public void ClientTimeouts_ReturnsARepeatedStatementTextOnce()
    {
        var monitoringTsql = "SELECT /* monitoring tool */ " + new string('c', 1500);
        var window = WindowWithTopStatement("fp-mon", monitoringTsql);
        var samples = new[]
        {
            Sample("fp-mon", monitoringTsql, 51),
            Sample("fp-mon", monitoringTsql, 52),
            Sample("fp-app", "SELECT 2", 53),
            Sample("fp-app", "SELECT 2", 54),
        };
        var reading = new AttentionReading(120, 10, 5, DateTime.UtcNow, Array.Empty<AttentionGroup>());

        var json = JsonSerializer.Serialize(ClientTimeoutsRead.Payload(reading, window, samples, 20, 2000), McpHelpers.JsonOptions);

        /* The 1.5 KB text is in the answer once, not twice. */
        Assert.Equal(1, CountOccurrences(json, monitoringTsql));

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal(monitoringTsql, root.GetProperty("top_statements")[0].GetProperty("sample_statement").GetString());
        var events = root.GetProperty("sampled_events");
        Assert.Equal(4, events.GetArrayLength());
        /* Events whose text sits in top_statements say where it is. */
        Assert.Equal(JsonValueKind.Null, events[0].GetProperty("statement").ValueKind);
        Assert.Equal("top_statements", events[0].GetProperty("statement_shown_in").GetString());
        Assert.Equal("top_statements", events[1].GetProperty("statement_shown_in").GetString());
        /* A text not in top_statements is written on its first event, and the second points at it. */
        Assert.Equal("SELECT 2", events[2].GetProperty("statement").GetString());
        Assert.Equal(JsonValueKind.Null, events[2].GetProperty("statement_shown_in").ValueKind);
        Assert.Equal(JsonValueKind.Null, events[3].GetProperty("statement").ValueKind);
        Assert.Equal("sampled_events[2]", events[3].GetProperty("statement_shown_in").GetString());
        Assert.Contains("returned once", root.GetProperty("samples_note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void ClientTimeouts_KeepsEveryDistinctTextAndANullTextAsItWas()
    {
        var window = WindowWithTopStatement("fp-a", "SELECT a");
        var samples = new[] { Sample("fp-a", "SELECT a variant", 1), Sample("fp-b", null, 2) };
        var reading = new AttentionReading(2, 5, 3, DateTime.UtcNow, Array.Empty<AttentionGroup>());

        using var doc = JsonDocument.Parse(
            JsonSerializer.Serialize(ClientTimeoutsRead.Payload(reading, window, samples, 20, 2000), McpHelpers.JsonOptions));
        var events = doc.RootElement.GetProperty("sampled_events");

        Assert.Equal("SELECT a variant", events[0].GetProperty("statement").GetString());
        Assert.Equal(JsonValueKind.Null, events[0].GetProperty("statement_shown_in").ValueKind);
        Assert.Equal(JsonValueKind.Null, events[1].GetProperty("statement").ValueKind);
        Assert.Equal(JsonValueKind.Null, events[1].GetProperty("statement_shown_in").ValueKind);
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var at = 0;
        while ((at = haystack.IndexOf(needle, at, StringComparison.Ordinal)) >= 0)
        {
            count++;
            at += needle.Length;
        }

        return count;
    }
}
