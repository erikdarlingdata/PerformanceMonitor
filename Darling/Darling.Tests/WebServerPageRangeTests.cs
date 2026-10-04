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

    /// <summary>A custom start and end maps to the reads' window: <c>as_of</c> is the end and <c>hours</c> is the span rounded
    /// UP to whole hours, so the fetch starts at or before the picked start. A span under an hour, a reversed pair, a future
    /// end and a span wider than the widest preset are refused; an end at "now" is live and names no <c>as_of</c>.</summary>
    [Fact]
    public void ACustomRange_MapsToAsOfAndWholeHours_AndRefusesWhatNoReadTakes()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("customMapping", out var r)) return;

        var found = r.GetProperty("found");
        var rounded = found.GetProperty("rounded");
        Assert.Equal(4, rounded.GetProperty("hours").GetInt32());
        Assert.Equal("2026-01-02T10:30:00.000Z", rounded.GetProperty("asOf").GetString());
        Assert.False(rounded.GetProperty("live").GetBoolean());
        Assert.Equal(4, found.GetProperty("exact").GetProperty("hours").GetInt32());
        Assert.Contains("at least one hour", found.GetProperty("subHour").GetProperty("error").GetString());
        Assert.Contains("after the start", found.GetProperty("reversed").GetProperty("error").GetString());
        Assert.Contains("future", found.GetProperty("future").GetProperty("error").GetString());
        Assert.Contains("7 days", found.GetProperty("tooWide").GetProperty("error").GetString());
        var live = found.GetProperty("live");
        Assert.True(live.GetProperty("live").GetBoolean());
        Assert.Equal(JsonValueKind.Null, live.GetProperty("asOf").ValueKind);
        Assert.Equal(5, live.GetProperty("hours").GetInt32());
    }

    /// <summary>Through the page: applying a past-end range re-reads the tab with the rounded hours and <c>as_of</c>, and a
    /// read that names no window (the scheduler pressure read) is left as it was.</summary>
    [Fact]
    public void ApplyingACustomRange_ReReadsTheTabAnchoredAtItsEnd()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("customPageReads", out var r)) return;

        var reads = Strings(r.GetProperty("found"), "reads");
        Assert.Equal(JsonValueKind.Null, r.GetProperty("found").GetProperty("err").ValueKind);
        Assert.Contains("/api/read/get_cpu_utilization?server=SRV1&hours=4&as_of=2026-01-02T10%3A30%3A00.000Z", reads);
        Assert.Contains("/api/read/get_top_queries_by_cpu?server=SRV1&hours=4&top=20&as_of=2026-01-02T10%3A30%3A00.000Z", reads);
        Assert.Contains("/api/read/get_cpu_scheduler_pressure?server=SRV1", reads);
        Assert.Empty(Strings(r, "errors"));
    }

    /// <summary>The 60 s poll leaves a fixed historical window alone; a live range, a tab click and a preset still read, and
    /// the pair belongs to the server it was picked for.</summary>
    [Fact]
    public void ThePoll_DoesNotRefreshAPastEndRange_ButKeepsItForTheServerAndRefreshesTheRest()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("customPoll", out var r)) return;

        var found = r.GetProperty("found");
        Assert.Empty(Strings(found, "pastPoll"));
        /* A rebuild that is not the poll (a tab click) still draws the same custom window. */
        Assert.Contains("/api/read/get_cpu_utilization?server=SRV1&hours=4&as_of=2026-01-02T10%3A30%3A00.000Z", Strings(found, "tabClick"));
        /* Another server starts on the preset. */
        Assert.Contains("/api/read/get_cpu_utilization?server=SRV2&hours=24", Strings(found, "other"));
        /* A range that ends now keeps refreshing, and names no as_of. */
        var live = Strings(found, "livePoll");
        Assert.Contains(live, f => f.StartsWith("/api/read/get_cpu_utilization?server=SRV3&hours=6", System.StringComparison.Ordinal));
        Assert.DoesNotContain(live, f => f.Contains("as_of=", System.StringComparison.Ordinal));
    }

    /// <summary>A chart over a custom range holds only the points inside the exact pair and its axis spans that pair; a read
    /// that takes no <c>as_of</c> shows the preset notice; a read for another server is untouched.</summary>
    [Fact]
    public void ACustomRange_TrimsToTheExactPair_AndNamesThePanelsThatKeepThePreset()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("customTrimAndNotice", out var r)) return;

        var fetches = Strings(r, "fetches");
        Assert.Contains("/api/read/get_cpu_utilization?server=SRV1&hours=4&as_of=2026-01-02T10%3A30%3A00.000Z", fetches);
        Assert.Contains("/api/read/get_read_latency?server=SRV1&hours=4", fetches);
        Assert.Contains("/api/read/get_cpu_utilization?server=SRV2&hours=4", fetches);
        Assert.Equal("This panel shows the last 4 hours, not the custom range: its read takes no end time.", Assert.Single(Strings(r, "notices")));
        /* 07:30, 09:00 and 10:00 are inside 07:15 to 10:30; 06:00 and 11:00 are not. The other server's chart keeps all five. */
        Assert.Equal(new[] { 3, 5 }, r.GetProperty("chartPoints").EnumerateArray().Select(e => e.GetInt32()).ToArray());
        Assert.Equal(new int?[] { 3, 3 }, r.GetProperty("chartHours").EnumerateArray().Select(e => (int?)e.GetInt32()).ToArray());
    }

    /// <summary>The reads that keep the preset are exactly the catalog's windowed reads that take no <c>as_of</c>, and every
    /// other windowed read takes one.</summary>
    [Fact]
    public void TheReadsWithoutAsOf_AreTheCatalogsWindowedReadsThatTakeNone()
    {
        var catalog = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        var windowedWithoutAsOf = Regex.Matches(catalog, @"\[""(\w+)""\] = R\((.*)")
            .Where(m => m.Groups[2].Value.Contains("PHours(", System.StringComparison.Ordinal) && !m.Groups[2].Value.Contains("PAsOf()", System.StringComparison.Ordinal))
            .Select(m => m.Groups[1].Value)
            .OrderBy(n => n, System.StringComparer.Ordinal)
            .ToArray();
        Assert.NotEmpty(windowedWithoutAsOf);

        var util = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "util.js");
        var set = Regex.Match(util, @"READS_WITHOUT_AS_OF = new Set\(\[(.*?)\]\)", RegexOptions.Singleline);
        Assert.True(set.Success, "util.js no longer declares READS_WITHOUT_AS_OF.");
        var declared = Regex.Matches(set.Groups[1].Value, @"""(\w+)""").Select(m => m.Groups[1].Value).OrderBy(n => n, System.StringComparer.Ordinal).ToArray();
        Assert.Equal(windowedWithoutAsOf, declared);
    }
}
