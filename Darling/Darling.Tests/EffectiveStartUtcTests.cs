/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
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
    [InlineData("DarlingMcpSessionTools.cs", 2)]
    [InlineData("DarlingMcpQueryHeatmapTools.cs", 1)]
    [InlineData("DarlingMcpQueryStoreRegressionTools.cs", 1)]
    public void EveryWindowFloorWrite_RoutesThroughTheSharedFormatter(string file, int minimumRouted)
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", file);
        var writes = source
            .Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.StartsWith("effective_start = ", StringComparison.Ordinal))
            .ToList();

        /* #4966: a tool that writes the shared notice (DarlingMcpWindowNotice) writes the value the helper formatted, and the
           helper's own formatting is pinned below. */
        bool IsRouted(string line) =>
            line.StartsWith("effective_start = McpHelpers.FormatEffectiveStart(", StringComparison.Ordinal)
            || line == "effective_start = notice.EffectiveStart,";
        var routed = writes.Count(IsRouted);
        var others = writes.Where(line => !IsRouted(line)).ToList();

        Assert.True(routed >= minimumRouted, $"{file} writes effective_start through McpHelpers.FormatEffectiveStart {routed} time(s); expected at least {minimumRouted}.");
        Assert.All(others, line => Assert.Equal("effective_start = (string?)null,", line));
    }

    /// <summary>
    /// #4966: the shared notice formats its <c>effective_start</c> through <see cref="McpHelpers.FormatEffectiveStart"/>, so every
    /// tool that writes <c>notice.EffectiveStart</c> prints UTC with the Z.
    /// </summary>
    [Fact]
    public void TheSharedNotice_FormatsItsEffectiveStart_ThroughTheSharedFormatter()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpWindowNotice.cs");
        Assert.Contains("McpHelpers.FormatEffectiveStart(RawWindowFloor.EffectiveStart(floor, requestedStart))", source, StringComparison.Ordinal);
        Assert.DoesNotContain(".ToString(\"o\"", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4966: the five Lite event-list tools write <c>effective_start</c> from the shared notice
    /// (<c>McpQueryTools.EventWindowNoticeAsync</c> -> <c>WindowNotice</c>, which prints through
    /// <see cref="McpHelpers.FormatEffectiveStart(DateTime)"/>), never from a bare <c>ToString("o")</c> of the store's naive floor.
    /// Each data answer writes the key once, from <c>notice.EffectiveStart</c>, and its empty answer carries the same notice under hints.
    /// </summary>
    [Theory]
    [InlineData("McpBlockingTools.cs", 4)]
    [InlineData("McpLongQueryTools.cs", 1)]
    public void EveryLiteEventListWindowFloorWrite_ComesFromTheSharedNotice(string file, int expectedTools)
    {
        var source = RepoFile.ReadRepoFile("Lite", "Mcp", file);
        var writes = source
            .Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.StartsWith("effective_start = ", StringComparison.Ordinal))
            .ToList();

        Assert.Equal(expectedTools, writes.Count);
        Assert.All(writes, line => Assert.Equal("effective_start = notice.EffectiveStart,", line));
        /* Two calls per tool: the data answer's, and the empty answer's. */
        Assert.Equal(expectedTools * 2, Regex.Matches(source, @"EventWindowNoticeAsync\(\s*\(\) => dataService\.GetQueryWindowFloorAsync").Count);
        Assert.Equal(expectedTools, Regex.Matches(source, @"emptyAnswer: true\)\)\.AsHints\(\)").Count);
    }

    /// <summary>
    /// #4966: the six Lite config and log tools (three config-change tools, the one-server <c>get_collection_log</c>,
    /// <c>get_plan_corrections</c> and <c>get_memory_pressure_events</c>) write <c>effective_start</c> from the shared notice
    /// (<c>McpQueryTools.WindowNoticeAsync</c> -> <c>WindowNotice</c>, which prints through
    /// <see cref="McpHelpers.FormatEffectiveStart(DateTime)"/>), never from a bare <c>ToString("o")</c> of the store's naive floor.
    /// Each data answer writes the key once, from <c>notice.EffectiveStart</c>; <paramref name="expectedHints"/> counts the
    /// <c>empty</c> answers that carry the same notice under hints (<c>get_collection_log</c> has two: the filtered one and the quiet window).
    /// None of them uses the event-time form: their probes read the column the rows are stamped on.
    /// <para>#4966 adds the three Lite aggregate tools to the same pin: <c>get_default_trace_events</c> (one write, one hints),
    /// <c>get_wait_stats</c> (one write, no hints: its no-rows answer is <c>unavailable</c> and stays bare) and
    /// <c>get_blocking_stats</c> in McpHealthTools.cs (one write for its two series, one hints on its empty answer).</para>
    /// </summary>
    [Theory]
    [InlineData("McpConfigHistoryTools.cs", 3, 3)]
    [InlineData("McpHealthTools.cs", 2, 3)]
    [InlineData("McpPlanCorrectionTools.cs", 1, 1)]
    [InlineData("McpMemoryTools.cs", 1, 1)]
    [InlineData("McpDefaultTraceTools.cs", 1, 1)]
    [InlineData("McpWaitTools.cs", 1, 0)]
    public void EveryLiteConfigAndLogWindowFloorWrite_ComesFromTheSharedNotice(string file, int expectedWrites, int expectedHints)
    {
        var source = RepoFile.ReadRepoFile("Lite", "Mcp", file);
        var writes = source
            .Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.StartsWith("effective_start = ", StringComparison.Ordinal))
            .ToList();

        Assert.Equal(expectedWrites, writes.Count);
        Assert.All(writes, line => Assert.Equal("effective_start = notice.EffectiveStart,", line));
        Assert.Equal(expectedHints, Regex.Matches(source, @"emptyAnswer: true\)\)\.AsHints\(\)").Count);
        Assert.DoesNotContain("EventWindowNoticeAsync", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4966, #5015: where a page's rows stop and start (<c>oldest_returned_*</c> and <c>newest_returned_*</c>: collection,
    /// event, deadlock and alert time) describe the window the page covers, so every tool that carries them prints them the
    /// same way as <c>effective_start</c>: UTC, with the Z, through the shared formatter, in both apps. The sweep reads
    /// every tool file of both apps, so a field added later is judged too: none of them can keep a bare
    /// <c>ToString("o")</c> of the store's naive instant. The one exception is the PostgreSQL log events' pair, which
    /// reads a <c>DateTime</c> the reader already marks UTC (<c>OccurredAtUtc</c>, which serializes with the Z). Row times
    /// keep the store's form.
    /// </summary>
    [Fact]
    public void EveryReturnedBoundWrite_OfBothApps_RoutesThroughTheSharedFormatter()
    {
        var found = 0;
        var unrouted = new List<string>();
        foreach (var directory in new[] { new[] { "Lite", "Mcp" }, new[] { "Darling", "PerformanceMonitor.Darling.Service", "Mcp" } })
        {
            foreach (var file in Directory.EnumerateFiles(RepoFile.PathTo(directory), "*.cs").Order(StringComparer.Ordinal))
            {
                foreach (var line in File.ReadAllLines(file).Select(l => l.Trim()))
                {
                    if (!Regex.IsMatch(line, @"^(oldest|newest)_returned_\w+ = "))
                    {
                        continue;
                    }

                    found++;
                    var routed = line.Contains("McpHelpers.FormatEffectiveStart(", StringComparison.Ordinal);
                    var alreadyUtcByType = Regex.IsMatch(line, @"^(oldest|newest)_returned_at = rows\[(\^1|0)\]\.OccurredAtUtc,$");
                    if (!routed && !alreadyUtcByType)
                    {
                        unrouted.Add($"{Path.GetFileName(file)}: {line}");
                    }
                }
            }
        }

        Assert.True(found >= 46, $"the sweep found {found} oldest/newest_returned_* writes; it should find every one in both apps (46 today).");
        Assert.True(unrouted.Count == 0, "These oldest_returned_* / newest_returned_* writes print the store's naive instant, without the Z:\n" + string.Join("\n", unrouted));
    }
}
