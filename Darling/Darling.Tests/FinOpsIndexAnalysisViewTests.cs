/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>Store-free pins for the get_finops index_analysis view: text cap, ordering, rounding, notes, payload shape and refusals.</summary>
public sealed class FinOpsIndexAnalysisViewTests
{
    private static IndexCleanupRecommendation Rec(string index, decimal gb = 1m, string db = "d", int id = 2,
        IndexCleanupResultKind kind = IndexCleanupResultKind.Disable, string script = "S", string def = "D", string? rule = null) => new()
    {
        DatabaseName = db, SchemaName = "dbo", TableName = "t", IndexName = index, IndexId = id,
        ResultKind = kind, Action = IndexCleanupAction.Disable, IndexSizeGb = gb, Script = script, OriginalIndexDefinition = def, ConsolidationRule = rule,
    };

    private static IndexCleanupRollup Roll(string? db, decimal max = 1m) => new() { DatabaseName = db, TotalMaxSavingsGb = max };

    private static JsonElement Build(IndexCleanupAnalysisResult r, int limit = 10, string? db = null, bool full = false) =>
        JsonDocument.Parse(DarlingMcpFinOpsTools.BuildIndexAnalysisPayload("srv", r, limit, db, full, null)).RootElement;

    [Fact]
    public void TruncateText_CutsOnlyPastTheCap_AndNullIsEmpty()
    {
        var cap = DarlingMcpFinOpsTools.IndexAnalysisTextCap;
        Assert.Equal(300, cap);
        var at = new string('x', 300);
        Assert.Equal((at, false), DarlingMcpFinOpsTools.TruncateIndexAnalysisText(at, cap));
        var over = DarlingMcpFinOpsTools.TruncateIndexAnalysisText(new string('x', 301), cap);
        Assert.True(over.Truncated);
        Assert.Equal(new string('x', 300) + "…", over.Text);
        Assert.Equal(("", false), DarlingMcpFinOpsTools.TruncateIndexAnalysisText(null, cap));
    }

    [Fact]
    public void TruncateText_NeverSplitsASurrogatePair_AtTheCap()
    {
        var text = new string('x', 299) + "\U0001F600" + new string('y', 20);
        Assert.True(char.IsHighSurrogate(text[299]));
        var (cut, truncated) = DarlingMcpFinOpsTools.TruncateIndexAnalysisText(text, 300);
        Assert.True(truncated);
        Assert.Equal(new string('x', 299) + "…", cut);
        var json = JsonSerializer.Serialize(cut);
        Assert.DoesNotContain('\uFFFD', json);
        Assert.DoesNotContain("\\ud83d", json, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RecommendationOrder_TwoReviewRowsOnOneIndex_OrderByConsolidationRule_NullsFirst()
    {
        var a = Rec("i", kind: IndexCleanupResultKind.Review, rule: "Reverse Duplicate");
        var b = Rec("i", kind: IndexCleanupResultKind.Review, rule: "Equal Except Filter");
        var c = Rec("i", kind: IndexCleanupResultKind.Review);
        foreach (var input in new[] { new[] { a, b, c }, new[] { c, b, a }, new[] { b, a, c } })
        {
            var ordered = DarlingMcpFinOpsTools.OrderIndexAnalysisRecommendations(input);
            Assert.Equal(new string?[] { null, "Equal Except Filter", "Reverse Duplicate" }, ordered.Select(r => r.ConsolidationRule).ToArray());
        }
    }

    [Fact]
    public void DatabaseName_EmptyOrWhitespace_IsNoFilter_AndTheWorkloadReasonFollowsOverall()
    {
        Assert.Null(DarlingMcpFinOpsTools.NormalizeIndexAnalysisDatabaseName(""));
        Assert.Null(DarlingMcpFinOpsTools.NormalizeIndexAnalysisDatabaseName("  "));
        Assert.Equal("d", DarlingMcpFinOpsTools.NormalizeIndexAnalysisDatabaseName("d"));
        var r = new IndexCleanupAnalysisResult { DatabaseRollups = [Roll("d")], Recommendations = [Rec("a")] };
        var blank = Build(r, db: " ");
        Assert.NotEqual(JsonValueKind.Null, blank.GetProperty("overall").ValueKind);
        Assert.Equal(JsonValueKind.Null, blank.GetProperty("database_name").ValueKind);
        Assert.NotEqual(JsonValueKind.Null, blank.GetProperty("overall_workload_reason").ValueKind);
        var one = Build(r, db: "d");
        Assert.Equal(JsonValueKind.Null, one.GetProperty("overall").ValueKind);
        Assert.Equal(JsonValueKind.Null, one.GetProperty("overall_workload_reason").ValueKind);
        Assert.Null(DarlingMcpFinOpsTools.IndexAnalysisOnlyParamMisuse("utilization", DarlingMcpFinOpsTools.NormalizeIndexAnalysisDatabaseName(""), false));
        var tool = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsTools.cs").ReplaceLineEndings("\n");
        Assert.True(tool.IndexOf("database_name = NormalizeIndexAnalysisDatabaseName(database_name);", StringComparison.Ordinal)
            < tool.IndexOf("IndexAnalysisOnlyParamMisuse(normalized", StringComparison.Ordinal));
    }

    [Fact]
    public void Guide_NamesTheDatabaseCap_AndTheUnboundedFullText()
    {
        var guide = DarlingMcpFinOpsTools.IndexAnalysisViewGuide;
        Assert.Contains($"at most {DarlingMcpFinOpsTools.MaxIndexAnalysisDatabases}", guide, StringComparison.Ordinal);
        Assert.Contains("databases_truncated", guide, StringComparison.Ordinal);
        Assert.Contains($"outside those {DarlingMcpFinOpsTools.MaxIndexAnalysisDatabases}", guide, StringComparison.Ordinal);
        Assert.Contains("in full with no size cap", guide, StringComparison.Ordinal);
    }

    [Fact]
    public void RecommendationOrder_IsSizeThenFullNameChain_SoEqualRowsOrderByIndexName()
    {
        var ordered = DarlingMcpFinOpsTools.OrderIndexAnalysisRecommendations(
            [Rec("b"), Rec("a"), Rec("big", 5m), Rec("z", 1m, "a")]);
        Assert.Equal(new[] { "big", "z", "a", "b" }, ordered.Select(r => r.IndexName).ToArray());
    }

    [Fact]
    public void DatabaseOrder_IsLargestReclaimThenName()
    {
        var ordered = DarlingMcpFinOpsTools.OrderIndexAnalysisDatabases([Roll("b"), Roll("a"), Roll("c", 9m)]);
        Assert.Equal(new[] { "c", "a", "b" }, ordered.Select(r => r.DatabaseName).ToArray());
    }

    [Fact]
    public void Gb_RoundsAwayFromZero_ToThreeDecimals()
    {
        var root = Build(new IndexCleanupAnalysisResult
        {
            DatabaseRollups = [Roll("d")],
            Recommendations = [Rec("a", 0.0005m)],
        });
        Assert.Equal(0.001m, root.GetProperty("recommendations")[0].GetProperty("index_size_gb").GetDecimal());
    }

    [Fact]
    public void Notes_AreUptimeThenDedupeThenAnalyzerNotes()
    {
        var root = Build(new IndexCleanupAnalysisResult
        {
            DatabaseRollups = [Roll("d")], UptimeWarning = true, DedupeOnlyApplied = true, Notes = ["n1", "n2"],
        });
        var notes = root.GetProperty("notes").EnumerateArray().Select(n => n.GetString()!).ToArray();
        Assert.Equal(4, notes.Length);
        Assert.StartsWith("Server uptime is under 14 days", notes[0], StringComparison.Ordinal);
        Assert.StartsWith("Dedupe-only mode is in effect", notes[1], StringComparison.Ordinal);
        Assert.Equal(new[] { "n1", "n2" }, notes.Skip(2).ToArray());
    }

    [Fact]
    public void OverallRow_HasNoWorkloadKeys_AndDatabaseRowsDo()
    {
        var root = Build(new IndexCleanupAnalysisResult { DatabaseRollups = [Roll("d")] });
        var overall = root.GetProperty("overall");
        Assert.False(overall.TryGetProperty("user_seeks", out _));
        Assert.False(overall.TryGetProperty("avg_lock_wait_ms", out _));
        Assert.True(root.GetProperty("databases")[0].TryGetProperty("avg_latch_wait_ms", out _));
        Assert.Contains("per database only", root.GetProperty("overall_workload_reason").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void CountsByAction_CountBeforeTheCut_AndTruncatedSaysSo()
    {
        var root = Build(new IndexCleanupAnalysisResult
        {
            DatabaseRollups = [Roll("d")], Recommendations = [Rec("a"), Rec("b"), Rec("c")],
        }, limit: 1);
        Assert.Equal(3, root.GetProperty("recommendation_count").GetInt32());
        Assert.Equal(3, root.GetProperty("counts_by_action").GetProperty("DISABLE").GetInt32());
        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(1, root.GetProperty("recommendations").GetArrayLength());
    }

    [Fact]
    public void TextCap_AppliesUnlessFullText_AndReviewRowsCarryNoScript()
    {
        var long400 = new string('q', 400);
        var r = new IndexCleanupAnalysisResult
        {
            DatabaseRollups = [Roll("d")],
            Recommendations = [Rec("a", script: long400, def: long400), Rec("r", 0.5m, kind: IndexCleanupResultKind.Review, script: long400)],
        };
        var cut = Build(r).GetProperty("recommendations");
        Assert.True(cut[0].GetProperty("script_truncated").GetBoolean());
        Assert.True(cut[0].GetProperty("definition_truncated").GetBoolean());
        Assert.Equal(301, cut[0].GetProperty("script").GetString()!.Length);
        Assert.Equal("", cut[1].GetProperty("script").GetString());
        var full = Build(r, full: true);
        Assert.Equal(400, full.GetProperty("recommendations")[0].GetProperty("script").GetString()!.Length);
        Assert.Equal(JsonValueKind.Null, full.GetProperty("text_cap_chars").ValueKind);
    }

    [Fact]
    public void DatabaseFilter_IsCaseInsensitive_DropsOverall_AndAnUnknownDatabaseIsEmpty()
    {
        var r = new IndexCleanupAnalysisResult
        {
            DatabaseRollups = [Roll("Alpha"), Roll("Beta")], Recommendations = [Rec("a", db: "Alpha"), Rec("b", db: "Beta")],
        };
        var root = Build(r, db: "alpha");
        Assert.Equal(JsonValueKind.Null, root.GetProperty("overall").ValueKind);
        Assert.Equal(1, root.GetProperty("database_count").GetInt32());
        Assert.Equal(1, root.GetProperty("recommendation_count").GetInt32());
        Assert.Equal("empty", DarlingMcpTestData.StatusOf(DarlingMcpFinOpsTools.BuildIndexAnalysisPayload("srv", r, 10, "nope", false, null)));
    }

    [Fact]
    public void NoRollups_AnswersTheCapabilityStatusWhenGiven_ElseEmpty()
    {
        var none = new IndexCleanupAnalysisResult();
        Assert.Equal("empty", DarlingMcpTestData.StatusOf(DarlingMcpFinOpsTools.BuildIndexAnalysisPayload("srv", none, 10, null, false, null)));
        Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(DarlingMcpFinOpsTools.BuildIndexAnalysisPayload("srv", none, 10, null, false, McpHelpers.Status("not_collected", "x"))));
    }

    [Fact]
    public void OtherViews_RefuseDatabaseNameAndFullText_IndexAnalysisAccepts()
    {
        foreach (var view in new[] { "utilization", "high_impact", "database_resources", "optimization" })
        {
            var db = DarlingMcpFinOpsTools.IndexAnalysisOnlyParamMisuse(view, "d", false);
            Assert.Equal("database_name", db!.Value.Parameter);
            Assert.Contains(view, db.Value.Message, StringComparison.Ordinal);
            var text = DarlingMcpFinOpsTools.IndexAnalysisOnlyParamMisuse(view, null, true);
            Assert.Equal("full_text", text!.Value.Parameter);
            Assert.Null(DarlingMcpFinOpsTools.IndexAnalysisOnlyParamMisuse(view, null, false));
        }

        Assert.Null(DarlingMcpFinOpsTools.IndexAnalysisOnlyParamMisuse("index_analysis", "d", true));
    }

    [Fact]
    public void Wiring_GuardPrecedesTheSwitch_AndTheViewReadsNeitherCultureNorDisplayText()
    {
        var tool = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsTools.cs").ReplaceLineEndings("\n");
        Assert.Contains("if (misuse is { } m) return McpHelpers.Refusal(m.Parameter, m.Message);", tool, StringComparison.Ordinal);
        Assert.True(tool.IndexOf("IndexAnalysisOnlyParamMisuse(normalized", StringComparison.Ordinal)
            < tool.IndexOf("switch (normalized)", StringComparison.Ordinal));
        var partial = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsTools.IndexAnalysis.cs");
        Assert.DoesNotContain("CurrentCulture", partial, StringComparison.Ordinal);
        Assert.DoesNotContain("AverageWaitMsText", partial, StringComparison.Ordinal);
        Assert.Contains("\"index_object_stats\"", partial, StringComparison.Ordinal);
    }

    [Fact]
    public void ServedHead_NamesTheView_AndStaysUnderTheTarget()
    {
        var served = McpToolGuideTests.Served("get_finops");
        Assert.Contains("index_analysis:", served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"served head is {served.Served.Length}");
        Assert.Contains("ordered by index size", served.Tail!, StringComparison.Ordinal);
    }
}
