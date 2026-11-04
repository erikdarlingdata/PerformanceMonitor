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
        var json = McpHelpers.Status(CollectorRuntimePrecondition.StatusWord, "The tool's own sentence.", DarlingRuntimePrecondition.StructuredReason(collector, status));
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
}
