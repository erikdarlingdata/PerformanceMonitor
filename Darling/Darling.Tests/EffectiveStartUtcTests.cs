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
}
