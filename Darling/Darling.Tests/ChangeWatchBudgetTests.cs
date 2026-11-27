/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>#8901: <c>get_change_watches</c> is held to its response budget, including when the newest watch fits only without the cut note.</summary>
public sealed class ChangeWatchBudgetTests
{
    private static JsonObject Watch(string id, int changes) => new()
    {
        ["watch_id"] = id,
        ["state"] = "closed",
        ["changes"] = new JsonArray(Enumerable.Range(0, changes).Select(i => (JsonNode)new JsonObject
        {
            ["change_time"] = $"2026-11-01T10:{i:D2}:00.0000000Z",
            ["kind"] = "plan_force",
            ["what"] = $"Query Store plan forced: query {1000 + i} in database SalesDb now uses plan {5000 + i}, forced by the plan forcing policy after a regression.",
        }).ToArray()),
    };

    private static JsonObject Payload(params JsonObject[] watches) => new()
    {
        ["server"] = "sql1",
        ["state"] = "all",
        ["watch_count"] = watches.Length,
        ["watches"] = new JsonArray(watches.Select(w => w.DeepClone()).ToArray()),
    };

    private static int Bytes(JsonNode node) => Encoding.UTF8.GetByteCount(node.ToJsonString());

    [Theory]
    [InlineData(2)]
    [InlineData(1)]
    public void TheNewestWatchThatFitsOnlyWithoutTheCutNote_IsStillCutByChanges_AndTheAnswerIsWithinTheBudget(int watchCount)
    {
        var newest = Watch("newest", 40);
        var payload = watchCount == 2 ? Payload(newest, Watch("older", 3)) : Payload(newest);

        /* The budget is the newest watch's bare size plus a little: it fits, but the shared cut's note does not. */
        var bare = Payload(newest);
        bare["watch_count"] = watchCount;
        var budget = Bytes(bare) + 20;

        var json = DarlingMcpChangeWatchTools.FitToBudget(payload, budget);

        Assert.True(Encoding.UTF8.GetByteCount(json) <= budget, $"answer is {Encoding.UTF8.GetByteCount(json)} bytes, budget {budget}.");
        var root = JsonDocument.Parse(json).RootElement;
        Assert.True(root.GetProperty("budget_truncated").GetBoolean());
        var watch = Assert.Single(root.GetProperty("watches").EnumerateArray());
        Assert.Equal("newest", watch.GetProperty("watch_id").GetString());
        Assert.True(watch.GetProperty("changes_left_out").GetInt32() > 0);
        Assert.Equal(40 - watch.GetProperty("changes_left_out").GetInt32(), watch.GetProperty("changes").GetArrayLength());
    }

    [Fact]
    public void AnAnswerThatFitsTheBudget_IsLeftWhole()
    {
        var payload = Payload(Watch("newest", 5), Watch("older", 3));
        var json = DarlingMcpChangeWatchTools.FitToBudget(payload, 1_000_000);
        var root = JsonDocument.Parse(json).RootElement;
        Assert.False(root.GetProperty("budget_truncated").GetBoolean());
        Assert.Equal(2, root.GetProperty("watches").GetArrayLength());
    }
}
