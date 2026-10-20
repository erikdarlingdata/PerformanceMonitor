/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// The export of the investigation cases that have feedback (#6236, part of #5632): the test set an operator replays their own
/// agent on. One JSON object per case, one case per line: <c>request_id</c>, <c>alert</c> (the request: server, family, first metric,
/// when and how often it fired), <c>brief</c> (the alert and the evidence the agent was given), <c>report</c> (what it wrote) and
/// <c>feedback</c> (the verdict, corrected verdict, missed step and comment people recorded).
///
/// <para><b>Masking is the report read's, not a copy of it.</b> Each case goes through
/// <see cref="InvestigationToolCore.ReportsAnswer"/> at the level the caller names, so a report's free text, its evidence, a stored
/// brief weaker than that level, and a person's login are masked exactly as <c>get_investigation_reports</c> masks them.</para>
/// </summary>
public static class InvestigationFeedbackExport
{
    /// <summary>The verdicts a person may record (the V191 CHECK).</summary>
    public static readonly IReadOnlyList<string> Verdicts = InvestigationFeedbackRules.Verdicts;

    /// <summary>Checks a verdict filter; null when it is blank or one of <see cref="Verdicts"/>, else the refusal in plain words.</summary>
    public static string? VerdictRefusal(string? verdict) =>
        string.IsNullOrWhiteSpace(verdict) || Verdicts.Contains(verdict.Trim(), StringComparer.Ordinal)
            ? null
            : $"A verdict filter is one of {string.Join(", ", Verdicts)}; '{verdict}' is not.";

    /// <summary>The cases of the filter as JSON lines (no line break inside one), newest request first, and whether the filter held more
    /// than <see cref="InvestigationRequestStore.FeedbackExportLimit"/> cases.</summary>
    public static async Task<(IReadOnlyList<string> Lines, bool Truncated)> ReadLinesAsync(
        InvestigationRequestStore store, DateTime? sinceUtc, DateTime? untilUtc, int? serverId, string? verdict,
        McpDataDetail mode, PseudonymKey? key, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(store);
        var read = await store.ReadFeedbackCasesAsync(
            sinceUtc, untilUtc, serverId, string.IsNullOrWhiteSpace(verdict) ? null : verdict.Trim(),
            InvestigationRequestStore.FeedbackExportLimit, cancellationToken);
        return (read.Cases.Select(c => Line(c, mode, key)).ToList(), read.Truncated);
    }

    /// <summary>One case as one JSON line, masked at <paramref name="mode"/>.</summary>
    public static string Line(InvestigationFeedbackCase export, McpDataDetail mode, PseudonymKey? key)
    {
        ArgumentNullException.ThrowIfNull(export);
        var answer = JsonNode.Parse(InvestigationToolCore.ReportsAnswer(
            new InvestigationReportsRead(
                [export.Request], [export.Report], [], export.Brief, Feedback: export.Feedback, EvidenceByBasis: true),
            null, mode, key))!;
        var report = (JsonObject)answer["reports"]!.AsArray()[0]!.DeepClone();
        var feedback = report["feedback"]?.DeepClone() ?? new JsonArray();
        report.Remove("feedback");
        report.Remove("labels");
        var line = new JsonObject
        {
            ["request_id"] = export.Request.RequestId.ToString(),
            ["alert"] = answer["requests"]!.AsArray()[0]!.DeepClone(),
            ["brief"] = answer["brief"]?.DeepClone(),
            ["report"] = report,
            ["feedback"] = feedback,
        };
        return line.ToJsonString(McpHelpers.JsonOptions);
    }
}
