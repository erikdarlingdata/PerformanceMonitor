/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7410: a runtime-precondition reply carries structured <c>hints</c> beside its unchanged <c>message</c>, so the web page can say one plain
/// sentence without reading the tool's text.
/// </summary>
public sealed class DarlingRuntimePreconditionReasonTests
{
    private static JsonElement Reason(string collector, string? status)
    {
        var json = McpHelpers.Status(CollectorRuntimePrecondition.StatusWord, "The tool's own sentence.", CollectorRuntimePrecondition.StructuredReason(collector, status));
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    [Fact]
    public void AMissingExtension_NamesTheDeclaredExtension_AndKeepsTheMessage()
    {
        var r = Reason("pg_buffer_usage", CollectorRuntimePrecondition.ExtensionMissingStatus);
        Assert.Equal("The tool's own sentence.", r.GetProperty("message").GetString());
        var hints = r.GetProperty("hints");
        Assert.Equal("extension_missing", hints.GetProperty("reason").GetString());
        Assert.Equal(new[] { "pg_buffercache" }, hints.GetProperty("extensions").EnumerateArray().Select(x => x.GetString()).ToArray());
        Assert.False(hints.GetProperty("restart_required").GetBoolean());
    }

    [Fact]
    public void AnExtensionThatMustBePreloaded_SaysARestartIsNeeded()
    {
        var hints = Reason("pg_predicate_stats", CollectorRuntimePrecondition.ExtensionMissingStatus).GetProperty("hints");
        Assert.Equal("pg_qualstats", hints.GetProperty("extensions")[0].GetString());
        Assert.True(hints.GetProperty("restart_required").GetBoolean());
    }

    [Fact]
    public void ASkippedSource_AndAMissingSession_HaveTheirOwnReasons_AndAnyOtherStatusHasNone()
    {
        Assert.Equal("source_unreadable", Reason("pg_column_stats", CollectorRuntimePrecondition.DegradedStatus).GetProperty("hints").GetProperty("reason").GetString());
        Assert.Equal("capture_session_missing", Reason("x", CollectorRuntimePrecondition.CaptureSessionMissingStatus).GetProperty("hints").GetProperty("reason").GetString());
        Assert.False(Reason("x", "SUCCESS").TryGetProperty("hints", out _));
        Assert.False(Reason("x", null).TryGetProperty("hints", out _));
    }

    /// <summary>#8555: one unmet facet is "precondition", several are "preconditions", never "precondition(s)".</summary>
    [Theory]
    [InlineData(1, "1 unsatisfied precondition, from pg_plan_capture_readiness, in the order they have to be fixed in: ")]
    [InlineData(2, "2 unsatisfied preconditions, from pg_plan_capture_readiness, in the order they have to be fixed in: ")]
    public void PlanCaptureThatIsNotReady_CountsItsPreconditions(int unmet, string expected)
    {
        var facets = Enumerable.Range(1, unmet).Select(i => "facet_" + i).ToArray();
        var message = JsonDocument.Parse(DarlingMcpPgPlanTools.PlanCaptureNotReadyStatus(facets)).RootElement.GetProperty("message").GetString()!;

        Assert.Contains(expected, message, System.StringComparison.Ordinal);
        Assert.DoesNotContain("(s)", message, System.StringComparison.Ordinal);
    }

    [Fact]
    public void PlanCaptureThatIsNotReady_CarriesTheReasonAndTheCountOnTheWire()
    {
        var r = JsonDocument.Parse(DarlingMcpPgPlanTools.PlanCaptureNotReadyStatus(new[] { "capture_threshold = -1", "extension_available = false" })).RootElement;
        Assert.Equal("precondition", r.GetProperty("status").GetString());
        /* #8572: counted, and in the causal order get_pg_plan_capture_readiness uses, not alphabetical. */
        Assert.Contains("2 unsatisfied preconditions", r.GetProperty("message").GetString()!, System.StringComparison.Ordinal);
        Assert.Contains("extension_available = false | capture_threshold = -1", r.GetProperty("message").GetString()!, System.StringComparison.Ordinal);
        var hints = r.GetProperty("hints");
        Assert.Equal("plan_capture_not_ready", hints.GetProperty("reason").GetString());
        Assert.Equal(2, hints.GetProperty("unsatisfied_count").GetInt32());
    }
}
