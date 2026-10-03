/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4966: <c>effective_start</c> names its instant as UTC, with the trailing Z, on every path of the window-floor
/// tools, in both apps. The served start is either the start that was asked for (UTC, so a plain "o" printed the Z)
/// or the floor read off the store (a naive instant, so it printed none), which made the zone marker come and go
/// with <c>window_truncated</c>. One formatter, <see cref="McpHelpers.FormatEffectiveStart"/>, now serves both.
/// </summary>
public sealed class EffectiveStartUtcTests
{
    /// <summary>A naive instant and a UTC instant print the same text, ending in Z, and the instant is never shifted.</summary>
    [Fact]
    public void FormatEffectiveStart_NamesTheInstantAsUtc_WhicheverKindItCameWith()
    {
        var instant = new DateTime(2026, 9, 3, 4, 5, 6, 789, DateTimeKind.Unspecified);
        const string expected = "2026-09-03T04:05:06.7890000Z";

        Assert.Equal(expected, McpHelpers.FormatEffectiveStart(instant));
        Assert.Equal(expected, McpHelpers.FormatEffectiveStart(DateTime.SpecifyKind(instant, DateTimeKind.Utc)));
        /* Only the kind is set: a Local-kind value is not shifted to UTC, so a naive floor is never mistaken for local time. */
        Assert.Equal(expected, McpHelpers.FormatEffectiveStart(DateTime.SpecifyKind(instant, DateTimeKind.Local)));
    }

    /// <summary>
    /// Every <c>effective_start</c> the window-floor tools write goes through the shared formatter: none is a bare
    /// <c>ToString("o")</c> of a value that is the store's naive floor on one path and the requested UTC start on
    /// another. The null write (a forced-raw read over a window raw no longer holds) is the one exception.
    /// </summary>
    [Theory]
    [InlineData("DarlingMcpDataTools.cs", 5)]
    [InlineData("DarlingMcpQueryStoreClutterTools.cs", 1)]
    public void EveryWindowFloorWrite_RoutesThroughTheSharedFormatter(string file, int minimumRouted)
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", file);
        var writes = source
            .Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.StartsWith("effective_start = ", StringComparison.Ordinal))
            .ToList();

        var routed = writes.Count(line => line.StartsWith("effective_start = McpHelpers.FormatEffectiveStart(", StringComparison.Ordinal));
        var others = writes.Where(line => !line.StartsWith("effective_start = McpHelpers.FormatEffectiveStart(", StringComparison.Ordinal)).ToList();

        Assert.True(routed >= minimumRouted, $"{file} writes effective_start through McpHelpers.FormatEffectiveStart {routed} time(s); expected at least {minimumRouted}.");
        Assert.All(others, line => Assert.Equal("effective_start = (string?)null,", line));
    }

    /// <summary>
    /// #4966: where a page's rows stop (<c>oldest_returned_collection_time</c>) describes the window the page covers, so
    /// every tool that carries it prints it the same way as <c>effective_start</c>: UTC, with the Z, through the shared
    /// formatter, in both apps (the two window tools, <c>get_collection_log</c> in its one-server and fleet forms, and
    /// <c>get_plan_corrections</c>, which writes null for a page that holds no recommendation). The newest row's time is
    /// the row's own and keeps the store's form.
    /// </summary>
    [Theory]
    [InlineData("Lite/Mcp/McpSessionTools.cs", 1)]
    [InlineData("Lite/Mcp/McpWaitTools.cs", 1)]
    [InlineData("Lite/Mcp/McpHealthTools.cs", 2)]
    [InlineData("Lite/Mcp/McpPlanCorrectionTools.cs", 1)]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSessionTools.cs", 2)]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs", 2)]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanCorrectionTools.cs", 1)]
    public void EveryOldestReturnedWrite_OfTheWindowTools_RoutesThroughTheSharedFormatter(string path, int expectedWrites)
    {
        var source = RepoFile.ReadRepoFile(path.Split('/'));
        var writes = source
            .Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.StartsWith("oldest_returned_collection_time = ", StringComparison.Ordinal))
            .ToList();

        Assert.Equal(expectedWrites, writes.Count);
        Assert.All(writes, line => Assert.True(
            line.StartsWith("oldest_returned_collection_time = McpHelpers.FormatEffectiveStart(", StringComparison.Ordinal)
                || line.StartsWith("oldest_returned_collection_time = page.Count == 0 ? null : McpHelpers.FormatEffectiveStart(", StringComparison.Ordinal),
            $"{path} writes oldest_returned_collection_time without the shared formatter: {line}"));
    }
}
