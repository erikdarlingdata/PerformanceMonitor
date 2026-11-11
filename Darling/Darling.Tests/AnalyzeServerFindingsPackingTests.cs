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

namespace Darling.Tests;

/// <summary>
/// #8569: how <c>analyze_server</c> packs findings into <see cref="McpResponseBudget.DefaultBytes"/>. A reply of 7,892 bytes
/// said only 1 of 7 findings fit, because the old cut stopped at the first finding that did not fit and one large second
/// finding left out every smaller one behind it. Both products call <see cref="AnalysisFindingsBudget.Pack"/> with findings
/// serialized the way their tool serializes them (severity, category, advice, drill_down, remediation fields), so these
/// cases run the packer on that shape. The Lite twin is <c>AnalyzeServerFindingsPackingTests</c> in Lite.Tests.
/// </summary>
public sealed class AnalyzeServerFindingsPackingTests
{
    private static JsonObject Finding(double severity, string category, string headline, int drillDownBytes, int remediationBytes = 200, int storyBytes = 300, int disclosureBytes = 0) => new()
    {
        ["severity"] = severity,
        ["category"] = category,
        ["story_text"] = new string('s', storyBytes),
        ["drill_down"] = drillDownBytes == 0 ? null : new string('d', drillDownBytes),
        ["advice"] = new JsonObject { ["headline"] = headline, ["investigation"] = "look at it", ["remediation"] = "fix it" },
        ["suggested_remediation_sql"] = null,
        ["remediation_command"] = new string('r', remediationBytes),
        ["destructive_risk_disclosure"] = disclosureBytes == 0 ? null : new string('x', disclosureBytes),
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
    public void ALargeSecondFinding_IsTrimmedToTheRoomLeft_AndTheSmallThirdStillRides()
    {
        /* The second is 30 KB: it fits a reply of its own, but not beside a 6 KB first. It is trimmed to the room left
           (its drill-down rows are cut), not skipped, because an as_of run persisted nothing to read it from. */
        var (packed, json, root) = Pack(
            Finding(0.9, "waits", "first", 6_000),
            Finding(0.8, "memory", "second", 30_000),
            Finding(0.7, "config", "third", 1_000));

        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
        Assert.Equal(["first", "second", "third"], Headlines(root));
        Assert.Equal(3, root["findings_returned"]!.GetValue<int>());
        Assert.Equal(3, root["finding_count"]!.GetValue<int>());
        Assert.Equal(1, packed.TrimmedCount);
        Assert.Empty(packed.LeftOut);
        Assert.Null(root["findings_left_out"]);
        Assert.True(root["truncated"]!.GetValue<bool>());

        var second = root["findings"]![1]!.AsObject();
        Assert.Null(second["drill_down"]);
        Assert.Equal(200, second["remediation_command"]!.GetValue<string>().Length);
        var cut = second["trimmed"]!["fields"]!.AsArray();
        Assert.Equal("drill_down", Assert.Single(cut)!["field"]!.GetValue<string>());
        Assert.Equal(30_002, cut[0]!["bytes"]!.GetValue<int>());
        Assert.Null(root["findings"]![0]!["trimmed"]);
        Assert.Null(root["findings"]![2]!["trimmed"]);

        var note = root["truncation_note"]!.GetValue<string>();
        Assert.Contains("1 did not fit in full and was trimmed", note, StringComparison.Ordinal);
        Assert.DoesNotContain("only", note, StringComparison.Ordinal);
        Assert.Contains("get_analysis_findings", note, StringComparison.Ordinal);
    }

    [Fact]
    public void EverySmallerFindingThatFits_IsReturnedInSeverityOrder_AndTheLargeOnesRideTrimmed()
    {
        var (packed, _, root) = Pack(
            Finding(0.95, "a", "a1", 10_000),
            Finding(0.90, "b", "b1", 28_000),
            Finding(0.85, "c", "c1", 5_000),
            Finding(0.80, "d", "d1", 26_000),
            Finding(0.75, "e", "e1", 4_000),
            Finding(0.70, "f", "f1", 500));

        Assert.Equal(["a1", "b1", "c1", "d1", "e1", "f1"], Headlines(root));
        Assert.Equal(2, packed.TrimmedCount);
        Assert.Empty(packed.LeftOut);
        Assert.NotNull(root["findings"]![1]!["trimmed"]);
        Assert.NotNull(root["findings"]![3]!["trimmed"]);
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
    public void ASecondFindingTooLargeEvenTrimmed_IsLeftOutAndNamed_AndTheThirdStillRides()
    {
        /* story_text is not a trimmable field, so the second cannot be cut down to fit. */
        var (packed, json, root) = Pack(
            Finding(0.9, "waits", "first", 6_000),
            Finding(0.8, "plan", "untrimmable", drillDownBytes: 10_000, storyBytes: 40_000),
            Finding(0.7, "config", "third", 1_000));

        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
        Assert.Equal(["first", "third"], Headlines(root));
        Assert.Equal(0, packed.TrimmedCount);
        var entry = Assert.Single(root["findings_left_out"]!.AsArray());
        Assert.Equal("plan", entry!["category"]!.GetValue<string>());
        Assert.Equal("untrimmable", entry["headline"]!.GetValue<string>());

        var note = root["truncation_note"]!.GetValue<string>();
        Assert.Contains("1 did not fit", note, StringComparison.Ordinal);
        Assert.Contains("findings_left_out", note, StringComparison.Ordinal);
        Assert.Contains("get_analysis_findings", note, StringComparison.Ordinal);
    }

    [Fact]
    public void ARemediationCommand_NeverRidesWithoutItsRiskDisclosure_WhenTheRemediationUnitIsTrimmed()
    {
        /* The second finding's disclosure (14 KB) is larger than its command (1.5 KB) and it has no drill-down rows, so only
           the remediation unit can be cut. Cutting the largest field first would drop the disclosure and keep the command. */
        var (packed, json, root) = Pack(
            Finding(0.9, "waits", "first", 20_000, disclosureBytes: 50),
            Finding(0.8, "config", "second", drillDownBytes: 0, remediationBytes: 1_500, disclosureBytes: 14_000),
            Finding(0.7, "memory", "third", 1_000, disclosureBytes: 50));

        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
        Assert.Equal(["first", "second", "third"], Headlines(root));
        Assert.Equal(1, packed.TrimmedCount);

        var second = root["findings"]![1]!.AsObject();
        Assert.Null(second["remediation_command"]);
        Assert.Null(second["destructive_risk_disclosure"]);
        var fields = second["trimmed"]!["fields"]!.AsArray().Select(c => c!["field"]!.GetValue<string>()).ToArray();
        Assert.Contains("remediation_command", fields);
        Assert.Contains("destructive_risk_disclosure", fields);

        foreach (var finding in root["findings"]!.AsArray())
        {
            Assert.False(
                finding!["remediation_command"] is not null && finding["destructive_risk_disclosure"] is null,
                "a finding carried its remediation command without its risk disclosure");
        }
    }

    [Fact]
    public void WhenCuttingTheDrillDownIsEnough_TheRemediationAndItsDisclosureStay()
    {
        var (_, _, root) = Pack(
            Finding(0.9, "waits", "first", 6_000),
            Finding(0.8, "config", "second", drillDownBytes: 30_000, remediationBytes: 1_500, disclosureBytes: 2_000));

        var second = root["findings"]![1]!.AsObject();
        Assert.Null(second["drill_down"]);
        Assert.Equal(1_500, second["remediation_command"]!.GetValue<string>().Length);
        Assert.Equal(2_000, second["destructive_risk_disclosure"]!.GetValue<string>().Length);
        Assert.Equal("drill_down", Assert.Single(second["trimmed"]!["fields"]!.AsArray())!["field"]!.GetValue<string>());
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

    [Fact]
    public void TwoLeftOutFindingsWithTheSameHeadline_AreToldApartByTheirStoryPath_TheKeyTheReturnedFindingsCarry()
    {
        /* #8628: two expensive queries read the same ("One expensive query dominated the server's work this
           window"). Left out by category and headline alone they were two identical lines. The labels come from the
           same helper both analyze_server tools use, built from the findings themselves. */
        AnalysisFinding BadActor(string hash) => new()
        {
            Category = "bad_actor",
            RootFactKey = "BAD_ACTOR_" + hash,
            StoryPath = "BAD_ACTOR_" + hash,
            StoryPathHash = "hash-" + hash,
        };

        var first = BadActor("0xAAAA");
        var second = BadActor("0xBBBB");
        var labelOne = AnalysisFindingsBudget.LabelFor(first);
        var labelTwo = AnalysisFindingsBudget.LabelFor(second);
        Assert.Equal(labelOne.Headline, labelTwo.Headline);

        JsonObject Node(AnalysisFinding f, double severity) => new()
        {
            ["severity"] = severity,
            ["category"] = f.Category,
            ["story_path"] = f.StoryPath,
            ["story_path_hash"] = f.StoryPathHash,
            ["story_text"] = new string('s', 40_000),
            ["advice"] = new JsonObject { ["headline"] = labelOne.Headline },
        };

        JsonNode[] nodes = [Node(first, 0.9), Node(second, 0.8)];
        var (packed, json) = AnalysisFindingsBudget.Pack(nodes, [labelOne, labelTwo], p => JsonSerializer.Serialize(p.Annotate(new JsonObject
        {
            ["findings_returned"] = p.KeptCount,
            ["findings"] = p.FindingsArray(),
        })));

        var root = JsonNode.Parse(json)!.AsObject();
        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
        Assert.Equal(2, packed.LeftOut.Count);
        var entries = root["findings_left_out"]!.AsArray();
        Assert.Equal(2, entries.Count);
        Assert.Equal(first.StoryPath, entries[0]!["story_path"]!.GetValue<string>());
        Assert.Equal(second.StoryPath, entries[1]!["story_path"]!.GetValue<string>());
        Assert.Equal(first.StoryPathHash, entries[0]!["story_path_hash"]!.GetValue<string>());
        Assert.NotEqual(entries[0]!.ToJsonString(), entries[1]!.ToJsonString());
    }
}
