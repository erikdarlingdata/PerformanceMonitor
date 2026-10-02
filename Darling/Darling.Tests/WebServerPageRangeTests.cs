/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using System.Linq;
using System.Text.Json;
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
    /// takes. A tab added later is built too, and a read it would over-ask fails here by range, tab and read. A read
    /// that comes late, after its tab has settled, is caught by a check of every read the run made, and what the tabs
    /// draw is mounted, so a notice or an error at an offered range fails by range and tab as well.
    /// </summary>
    [Fact]
    public void EveryOfferedRange_IsOneEveryRangedReadOnEveryTabTakes()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("offeredRanges:" + McpHelpers.MaxHoursBack, out var r)) return;

        var offered = r.GetProperty("offered").EnumerateArray().Select(e => e.GetInt32()).ToArray();
        Assert.NotEmpty(offered);

        var beyond = Strings(r, "beyond");
        Assert.True(
            beyond.Length == 0,
            $"The Range offers {string.Join(", ", offered)} hours, and {beyond.Length} ranged reads would be asked for more"
            + $" than {McpHelpers.MaxHoursBack} hours, the widest window the capped reads take:\n" + string.Join("\n", beyond.Take(40)));

        /* The list above is each tab's reads, taken right after that tab settled. A read a tab makes later (behind a
           timer, say) reaches the run's list of every fetch without reaching a tab's, and the last tab's has nothing
           after it to be counted in. So the whole list is checked as well: no read the page makes asks for more than the
           capped reads take, whether or not it can be tied to a tab. */
        var tooWide = Strings(r, "fetches").Where(f => HoursAsked(f) > McpHelpers.MaxHoursBack).ToArray();
        Assert.True(
            tooWide.Length == 0,
            $"{tooWide.Length} reads the page made asked for more than {McpHelpers.MaxHoursBack} hours, the widest window the"
            + " capped reads take (this list is every read the run made, so a read that cannot be tied to a tab is in it):\n"
            + string.Join("\n", tooWide.Take(40)));

        Assert.Empty(r.GetProperty("rejections").EnumerateArray());

        /* Every tab's drawing is mounted, so what the page shows at an offered range is seen. Each offered range is one
           the reads take, so no panel may need a notice that it asked again for fewer hours, and none may fail. The
           strips say which range and tab drew them. */
        var notices = Strings(r, "notices");
        Assert.True(
            notices.Length == 0,
            $"{notices.Length} notices were shown at an offered range, where no read is refused:\n" + string.Join("\n", notices.Take(40)));
        var errors = Strings(r, "errors");
        Assert.True(
            errors.Length == 0,
            $"{errors.Length} errors were shown at an offered range:\n" + string.Join("\n", errors.Take(40)));
        Assert.Equal(0, r.GetProperty("loading").GetInt32());

        /* "Nothing over-asked" must not pass because nothing was asked: each range builds the tabs of both registries,
           and named ranged reads from each are seen at that range's hours. */
        var observed = r.GetProperty("observed").EnumerateArray().Select(e => e.GetString()!).ToArray();
        foreach (var hours in offered)
        {
            foreach (var (engine, reads) in s_witnesses)
            {
                var seen = observed.Where(o => o.StartsWith($"{hours} {engine} ", System.StringComparison.Ordinal)).ToArray();
                foreach (var read in reads)
                {
                    Assert.True(
                        seen.Contains($"{hours} {engine} {read}"),
                        $"At {hours} hours the {engine} tabs never asked {read} for a window ({seen.Length} ranged reads seen there).");
                }
            }
        }
    }

    private static string[] Strings(JsonElement node, string name) =>
        node.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    private static readonly Regex s_hoursParam = new(@"[?&]hours=([0-9]+(?:\.[0-9]+)?)", RegexOptions.CultureInvariant);

    /// <summary>The hours a fetched URL asks for, or 0 when it takes no window.</summary>
    private static double HoursAsked(string fetched) =>
        s_hoursParam.Match(fetched) is { Success: true } hours ? double.Parse(hours.Groups[1].Value, CultureInfo.InvariantCulture) : 0;

    /// <summary>Ranged reads each registry's tabs make at every range: the census must see them asked.</summary>
    private static readonly (string Engine, string[] Reads)[] s_witnesses =
    [
        ("SQL Server", ["get_cpu_utilization", "get_wait_trend", "get_blocking_stats", "get_current_waits_trend", "get_collection_log"]),
        ("PostgreSQL", ["get_pg_blocking", "get_pg_io_stats", "get_collection_log"]),
    ];

    /// <summary>
    /// The Blocking tab's note says how far back the page reaches and where the rest of the history is. Its number is
    /// the widest preset, which the page hands to <c>tabNote</c>, so narrowing the presets cannot leave the note
    /// wrong. <c>get_blocking_stats</c> takes any window and draws both severity charts, and its tables keep more than
    /// the page shows. No other tab carries the pointer: the PostgreSQL tabs' only read that takes any window is the
    /// collection log, which asks for the newest 200 runs.
    /// </summary>
    [Fact]
    public void TheBlockingTabNote_NamesTheWidestOfferedRange()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("offeredRanges:" + McpHelpers.MaxHoursBack, out var r)) return;

        var widest = r.GetProperty("offered").EnumerateArray().Max(e => e.GetInt32());
        Assert.True(widest % 24 == 0, $"The widest preset is {widest} hours, not a whole number of days, so this pin needs remapping.");
        var days = widest / 24;
        var notes = r.GetProperty("notes").EnumerateArray().Select(e => e.GetString()!).ToArray();

        var blocking = Assert.Single(notes, n => n.StartsWith("SQL Server blocking: ", System.StringComparison.Ordinal));
        Assert.Contains(
            $"This page shows at most {days} day{(days == 1 ? "" : "s")}. A Custom View can show more blocking and deadlock history.",
            blocking,
            System.StringComparison.Ordinal);
        Assert.Single(notes, n => n.Contains("A Custom View can show more", System.StringComparison.Ordinal));
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
