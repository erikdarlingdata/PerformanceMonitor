/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9873: a frozen evidence row says when the statement filter withheld its whole deadlock graph, blocked-process report or plan
/// (<c>graph_withheld</c>, <c>report_withheld</c>, <c>good_plan_withheld</c>, <c>bad_plan_withheld</c>). The evidence an agent reads
/// (<c>get_investigation_evidence</c>, and the brief both products compose from the same rows) goes through
/// <c>InvestigationBriefComposer.EvidenceSection</c>, which lists no fields: these facts keep that so, at both data-detail levels.
/// </summary>
public sealed class InvestigationEvidenceWithheldReadTests
{
    private static readonly DateTime CapturedAt = new(2026, 11, 27, 12, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData("deadlocks", "graph_withheld", InvestigationEvidenceWithheld.GraphReason)]
    [InlineData("head_blocker", "report_withheld", InvestigationEvidenceWithheld.ReportReason)]
    [InlineData("regression_targets", "good_plan_withheld", InvestigationEvidenceWithheld.PlanReason)]
    [InlineData("regression_targets", "bad_plan_withheld", InvestigationEvidenceWithheld.PlanReason)]
    public void AWithheldField_ReachesTheAgentWithItsReason_AtEveryDataDetailLevel(string section, string field, string reason)
    {
        var content = new JsonObject
        {
            ["rows"] = new JsonArray(new JsonObject { ["spid"] = 55, [field] = reason }),
        }.ToJsonString();
        var evidence = new InvestigationBriefEvidence(section, "ready", CapturedAt, content);

        foreach (var mode in new[] { McpDataDetail.Full, McpDataDetail.MetadataOnly })
        {
            var served = InvestigationBriefComposer.EvidenceSection(evidence, mode);

            var row = Assert.Single(served["content"]!["rows"]!.AsArray())!;
            Assert.Equal(reason, row[field]!.GetValue<string>());
            Assert.Equal(55, row["spid"]!.GetValue<int>());
        }
    }
}
