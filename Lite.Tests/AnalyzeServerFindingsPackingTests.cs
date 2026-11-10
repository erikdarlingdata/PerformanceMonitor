/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8569: how <c>analyze_server</c> packs findings into <see cref="McpResponseBudget.DefaultBytes"/>. A reply of 7,892 bytes
/// said only 1 of 7 findings fit, because the old cut stopped at the first finding that did not fit and one large second
/// finding left out every smaller one behind it. Both products call <see cref="AnalysisFindingsBudget.Pack"/> with findings
/// serialized the way their tool serializes them (severity, category, advice, drill_down, remediation fields), so these
/// cases run the packer on that shape. The Darling twin is <c>AnalyzeServerFindingsPackingTests</c> in Darling.Tests.
/// </summary>
public sealed class AnalyzeServerFindingsPackingTests
{
    private static JsonObject Finding(double severity, string category, string headline, int drillDownBytes, int remediationBytes = 200, int storyBytes = 300) => new()
    {
        ["severity"] = severity,
        ["category"] = category,
        ["story_text"] = new string('s', storyBytes),
        ["drill_down"] = drillDownBytes == 0 ? null : new string('d', drillDownBytes),
        ["advice"] = new JsonObject { ["headline"] = headline, ["investigation"] = "look at it", ["remediation"] = "fix it" },
        ["suggested_remediation_sql"] = null,
        ["remediation_command"] = new string('r', remediationBytes),
    };

    private static (PackedFindings Packed, string Json, JsonObject Root) Pack(params JsonObject[] findings)
    {
        var labels = findings.Select(f => new FindingLabel(f["category"]!.GetValue<string>(), f["advice"]!["headline"]!.GetValue<string>())).ToList();
        var (packed, json) = AnalysisFindingsBudget.Pack(findings, labels, p => JsonSerializer.Serialize(p.Annotate(new JsonObject
        {
            ["finding_count"] = findings.Length,
            ["findings_returned"] = p.KeptCount,
            ["truncated"] = p.LeftOut.Count > 0 || p.TrimmedCount > 0,
            ["truncation_note"] = AnalysisFindingsBudget.TruncationNote(findings.Length, p, persisted: true),
            ["findings"] = p.FindingsArray(),
        })));
        return (packed, json, JsonNode.Parse(json)!.AsObject());
    }

    private static string[] Headlines(JsonObject root) =>
        root["findings"]!.AsArray().Select(f => f!["advice"]!["headline"]!.GetValue<string>()).ToArray();

    [Fact]
    public void ALargeSecondFinding_DoesNotCostTheSmallThirdItsPlace_AndTheNoteNamesTheSecond()
    {
        /* The second is 30 KB: it fits a reply of its own, but not beside a 6 KB first. The third is small. */
        var (_, json, root) = Pack(
            Finding(0.9, "waits", "first", 6_000),
            Finding(0.8, "memory", "second", 30_000),
            Finding(0.7, "config", "third", 1_000));

        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
        Assert.Equal(["first", "third"], Headlines(root));
        Assert.Equal(2, root["findings_returned"]!.GetValue<int>());
        Assert.Equal(3, root["finding_count"]!.GetValue<int>());
        Assert.True(root["truncated"]!.GetValue<bool>());

        var leftOut = root["findings_left_out"]!.AsArray();
        var entry = Assert.Single(leftOut);
        Assert.Equal("memory", entry!["category"]!.GetValue<string>());
        Assert.Equal("second", entry["headline"]!.GetValue<string>());

        var note = root["truncation_note"]!.GetValue<string>();
        Assert.Contains("1 did not fit", note, StringComparison.Ordinal);
        Assert.Contains("findings_left_out", note, StringComparison.Ordinal);
        Assert.DoesNotContain("only", note, StringComparison.Ordinal);
        Assert.Contains("get_analysis_findings", note, StringComparison.Ordinal);
    }

    [Fact]
    public void EverySmallerFindingThatFits_IsReturnedInSeverityOrder_PastSeveralThatDoNot()
    {
        var (_, _, root) = Pack(
            Finding(0.95, "a", "a1", 10_000),
            Finding(0.90, "b", "b1", 28_000),
            Finding(0.85, "c", "c1", 5_000),
            Finding(0.80, "d", "d1", 26_000),
            Finding(0.75, "e", "e1", 4_000),
            Finding(0.70, "f", "f1", 500));

        Assert.Equal(["a1", "c1", "e1", "f1"], Headlines(root));
        Assert.Equal(["b1", "d1"], root["findings_left_out"]!.AsArray().Select(e => e!["headline"]!.GetValue<string>()).ToArray());
    }

    [Fact]
    public void AFindingLargerThanTheWholeBudget_ComesBackTrimmedAndFlagged_NotDropped()
    {
        var (packed, json, root) = Pack(
            Finding(0.9, "plan", "huge", drillDownBytes: 50_000, remediationBytes: 3_000),
            Finding(0.8, "waits", "small", 1_000));

        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
        Assert.Equal(["huge", "small"], Headlines(root));
        Assert.Equal(1, packed.TrimmedCount);
        Assert.Empty(packed.LeftOut);
        Assert.Null(root["findings_left_out"]);

        var huge = root["findings"]![0]!.AsObject();
        Assert.Null(huge["drill_down"]);
        Assert.Equal(3_000, huge["remediation_command"]!.GetValue<string>().Length);
        var trimmed = huge["trimmed"]!.AsObject();
        var cut = trimmed["fields"]!.AsArray();
        Assert.Equal("drill_down", cut[0]!["field"]!.GetValue<string>());
        Assert.True(cut[0]!["bytes"]!.GetValue<int>() >= 50_000);
        Assert.Contains("response budget", trimmed["note"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.Null(root["findings"]![1]!["trimmed"]);
        Assert.True(root["truncated"]!.GetValue<bool>());
        Assert.Contains("trimmed", root["truncation_note"]!.GetValue<string>(), StringComparison.Ordinal);
    }

    [Fact]
    public void AFindingTooLargeEvenTrimmed_IsDropped_AndListedWithTheOthers()
    {
        /* story_text is not a trimmable field, so this one cannot be cut down to fit. */
        var (_, json, root) = Pack(
            Finding(0.9, "plan", "untrimmable", drillDownBytes: 10_000, storyBytes: 40_000),
            Finding(0.8, "waits", "small", 1_000));

        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
        Assert.Equal(["small"], Headlines(root));
        Assert.Equal("untrimmable", Assert.Single(root["findings_left_out"]!.AsArray())!["headline"]!.GetValue<string>());
    }

    [Fact]
    public void WhenEveryFindingFits_TheReplyIsUntouched_NoLeftOutList_NoNote()
    {
        var (packed, _, root) = Pack(
            Finding(0.9, "a", "a1", 2_000),
            Finding(0.8, "b", "b1", 2_000));

        Assert.Equal(2, packed.KeptCount);
        Assert.Empty(packed.LeftOut);
        Assert.False(root["truncated"]!.GetValue<bool>());
        Assert.Null(root["truncation_note"]);
        Assert.Null(root["findings_left_out"]);
        Assert.Null(root["findings"]![0]!["trimmed"]);
    }
}
