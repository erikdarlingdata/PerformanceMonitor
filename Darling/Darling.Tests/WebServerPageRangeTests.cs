/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The server page's Range offers only windows its tabs can serve. All but three of the page's ranged reads refuse
/// more than <see cref="McpHelpers.MaxHoursBack"/> hours, so a 30-day choice came back as 7 days on nearly every
/// panel, each with a notice saying it had asked again. The first test runs every offered range through every tab
/// of both registries under Node (<c>web-kept-history-harness.mjs</c>), against reads that refuse a wider window the
/// way the real ones do. The second reads the presets from the source, so it holds where Node is not installed.
/// </summary>
public sealed class WebServerPageRangeTests
{
    /// <summary>
    /// Every range the page offers, on every tab of both registries: no ranged read is asked for more hours than it
    /// takes. A tab added later is built too, and a read it would over-ask fails here by range, tab and read.
    /// </summary>
    [Fact]
    public void EveryOfferedRange_IsOneEveryRangedReadOnEveryTabTakes()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("offeredRanges:" + McpHelpers.MaxHoursBack, out var r)) return;

        var offered = r.GetProperty("offered").EnumerateArray().Select(e => e.GetInt32()).ToArray();
        Assert.NotEmpty(offered);

        var beyond = r.GetProperty("beyond").EnumerateArray().Select(e => e.GetString()!).ToArray();
        Assert.True(
            beyond.Length == 0,
            $"The Range offers {string.Join(", ", offered)} hours, and {beyond.Length} ranged reads would be asked for more"
            + $" than {McpHelpers.MaxHoursBack} hours, the widest window the capped reads take:\n" + string.Join("\n", beyond.Take(40)));

        Assert.Empty(r.GetProperty("rejections").EnumerateArray());
    }

    /// <summary>The same rule read from <c>pages/server.js</c>: each preset is at least an hour and no wider than
    /// <see cref="McpHelpers.MaxHoursBack"/>.</summary>
    [Fact]
    public void TheOfferedRanges_InTheSource_AreNoWiderThanTheReadsTake()
    {
        var server = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server.js");
        var options = Regex.Match(server, @"const RANGE_OPTIONS = \[(.*?)\];", RegexOptions.Singleline);
        Assert.True(options.Success, "pages/server.js no longer declares RANGE_OPTIONS.");

        var hours = Regex.Matches(options.Groups[1].Value, @"hours:\s*([0-9\s*]+),")
            .Select(m => m.Groups[1].Value.Split('*').Aggregate(1, (product, factor) => product * int.Parse(factor.Trim())))
            .ToArray();

        Assert.NotEmpty(hours);
        Assert.All(hours, h => Assert.InRange(h, 1, McpHelpers.MaxHoursBack));
    }
}
