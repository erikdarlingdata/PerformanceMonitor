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
using PerformanceMonitor.Darling.Service;
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
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("offeredRanges:" + McpHelpers.MaxHoursBack, out var r, CatalogJson())) return;

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
        var tooWide = Strings(r, "fetches").Where(f => HoursAsked(f) > ReachOf(f)).ToArray();
        Assert.True(
            tooWide.Length == 0,
            $"{tooWide.Length} reads the page made asked for more hours than the read takes ({McpHelpers.MaxHoursBack} unless the"
            + " read's row in WebReadReach says more; this list is every read the run made, so a read that cannot be tied to a tab is in it):\n"
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

    /// <summary>The real <c>/api/catalog</c> body, which the harness serves to the page: each read's <c>max_hours</c> is the one the
    /// service serves, so a tab's reach in these tests is the product's, not a copy.</summary>
    private static string CatalogJson() => DarlingWebEndpoints.BuildCatalogNode().ToJsonString();

    private static string[] Strings(JsonElement node, string name) =>
        node.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    /// <summary>The hours the read in a fetched URL takes: its row in <see cref="WebReadReach"/>, else the common 168.</summary>
    private static int ReachOf(string fetched)
    {
        var read = Regex.Match(fetched, @"/api/read/([a-z_0-9]+)", RegexOptions.CultureInvariant);
        return read.Success && WebReadReach.All.TryGetValue(read.Groups[1].Value, out var row) ? row.MaxHours : McpHelpers.MaxHoursBack;
    }

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
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("offeredRanges:" + McpHelpers.MaxHoursBack, out var r, CatalogJson())) return;

        /* The Blocking tab's own reach (#5562 review r1 M4): the number its note names is the widest range THIS tab offers. */
        var widest = r.GetProperty("found").GetProperty("reaches").GetProperty("SQL Server blocking").GetInt32();
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

    /// <summary>The same rule read from <c>pages/server.js</c> (#5562 review r1 M4): the page holds no reach of its own. A tab's
    /// reach is the catalog's, read through <c>readsReachHours</c>; it is handed to the picker so a longer range is greyed out with
    /// the tab's reason; and the whole-hour presets come from the picker module's presets, never a list of its own.</summary>
    [Fact]
    public void TheOfferedRanges_InTheSource_AreTheTabsReachFromTheCatalog()
    {
        var server = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server.js");
        Assert.DoesNotContain("PAGE_REACH_HOURS", server, System.StringComparison.Ordinal);
        Assert.Contains("readsReachHours(", server, System.StringComparison.Ordinal);
        Assert.Contains("reachHours: tabReach(null, server)", server, System.StringComparison.Ordinal);
        Assert.Contains("reachMessage:", server, System.StringComparison.Ordinal);
        Assert.Matches(@"const RANGE_OPTIONS = ROLLING_PRESETS", server);
        Assert.DoesNotContain("hours <= ", server.Split("function rangeOption")[0], System.StringComparison.Ordinal);
    }

    /// <summary>
    /// A tab does not hand-type its hours (#5562 review r1 M4, ruling R2). Every tab names the windowed reads it makes
    /// (<c>reachReads</c>), each name is a read the web serves, and no tab object, the registry or the server page carries a
    /// number of hours as a reach: the only source of a number is the catalog's <c>max_hours</c>.
    /// </summary>
    [Fact]
    public void ATab_NamesItsReads_AndNeverTypesItsHours()
    {
        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        var findings = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "analysis-findings.js");
        var server = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server.js");

        var hand = new Regex(@"\b(reach|reachHours|reachReads|maxHours|max_hours)\s*[:=]\s*[0-9]", RegexOptions.CultureInvariant);
        foreach (var (file, text) in new[] { ("server-tabs.js", tabs), ("analysis-findings.js", findings), ("server.js", server) })
        {
            var typed = hand.Matches(text).Select(m => m.Value).ToArray();
            Assert.True(typed.Length == 0, $"{file} types a reach as a number ({string.Join("; ", typed)}); a tab's reach is the catalog's max_hours of its reachReads.");
        }

        /* Each registry tab declares reachReads (the whole registry is 12 + 8 tabs; the Recommendations tab lives in its own module). */
        var declared = Regex.Matches(tabs, @"reachReads:\s*\[([^\]]*)\]", RegexOptions.CultureInvariant);
        Assert.True(declared.Count >= 15, $"Only {declared.Count} tabs declare reachReads.");
        Assert.Contains("reachReads: [\"get_analysis_findings\"]", findings, System.StringComparison.Ordinal);

        var served = DarlingWebEndpoints.CatalogDescriptors.Keys.ToHashSet(System.StringComparer.Ordinal);
        foreach (var name in declared.SelectMany(d => Regex.Matches(d.Groups[1].Value, "\"([a-z_]+)\"").Select(m => m.Groups[1].Value)).Distinct())
        {
            Assert.True(served.Contains(name), $"A tab's reachReads names {name}, which the web does not serve.");
        }
    }

    /// <summary>
    /// A tab's reach is the smallest <c>max_hours</c> among the reads it makes, from the real catalog. The I/O tab is made of
    /// two 30-day trends and offers 30 days; the Activity tab shows the perfmon trend (7 days) beside them and stays at 7; the
    /// Queries tab shows the query heatmap (7 days) and stays at 7; a tab with a list read stays at 7; and every tab's reach
    /// equals the minimum computed here from the read table, so a tab with two reads takes the smaller. A tab that makes a
    /// windowed read its <c>reachReads</c> does not name fails the run, because an unnamed read could lift the tab past it.
    /// </summary>
    [Fact]
    public void ATabsReach_IsTheSmallestOfItsReads_FromTheCatalog()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("offeredRanges:" + McpHelpers.MaxHoursBack, out var r, CatalogJson())) return;

        var found = r.GetProperty("found");
        var reaches = found.GetProperty("reaches");
        Assert.Equal(720, reaches.GetProperty("SQL Server io").GetInt32());
        Assert.Equal(McpHelpers.MaxHoursBack, reaches.GetProperty("SQL Server activity").GetInt32());
        Assert.Equal(McpHelpers.MaxHoursBack, reaches.GetProperty("SQL Server queries").GetInt32());
        Assert.Equal(McpHelpers.MaxHoursBack, reaches.GetProperty("SQL Server blocking").GetInt32());
        Assert.Equal(McpHelpers.MaxHoursBack, reaches.GetProperty("SQL Server config").GetInt32());

        /* The I/O tab offers 30 days and the Activity tab does not. */
        var offeredBy = found.GetProperty("offeredBy");
        Assert.Contains(720, offeredBy.GetProperty("SQL Server io").EnumerateArray().Select(e => e.GetInt32()));
        Assert.DoesNotContain(720, offeredBy.GetProperty("SQL Server activity").EnumerateArray().Select(e => e.GetInt32()));

        /* Every tab: the harness's reach equals the minimum of the reads' table rows (a read without a row is 168). */
        foreach (var tab in reaches.EnumerateObject())
        {
            var reads = TabReads(tab.Name);
            var expected = reads.Length == 0
                ? McpHelpers.MaxHoursBack
                : reads.Min(read => WebReadReach.All.TryGetValue(read, out var row) ? row.MaxHours : McpHelpers.MaxHoursBack);
            Assert.True(expected == tab.Value.GetInt32(), $"{tab.Name}: the page gave {tab.Value.GetInt32()} hours, the smallest of its reads is {expected}.");
        }

        var undeclared = Strings(found, "undeclared");
        Assert.True(undeclared.Length == 0, "These tabs made a windowed read their reachReads does not name:\n" + string.Join("\n", undeclared));
    }

    /// <summary>The reads a tab declares, from the registry source: the tab key is "engine id" as the harness names it.</summary>
    private static string[] TabReads(string key)
    {
        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        var postgres = key.StartsWith("PostgreSQL ", System.StringComparison.Ordinal);
        var id = key[(key.LastIndexOf(' ') + 1)..];
        var registry = tabs[tabs.IndexOf("export const POSTGRES_TABS", System.StringComparison.Ordinal)..];
        var sql = tabs[..tabs.IndexOf("export const POSTGRES_TABS", System.StringComparison.Ordinal)];
        var source = postgres ? registry : sql;
        if (key.StartsWith("SQL Server recommendations", System.StringComparison.Ordinal))
        {
            return ["get_analysis_findings"];
        }

        var match = Regex.Match(source, "    id: \"" + Regex.Escape(id) + "\",.*?(?:reachReads: \\[([^\\]]*)\\])?\\s*,?\\s*build", RegexOptions.Singleline | RegexOptions.CultureInvariant);
        return match.Success && match.Groups[1].Success
            ? Regex.Matches(match.Groups[1].Value, "\"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToArray()
            : [];
    }

    /// <summary>
    /// A range carried to a tab that reads less (#5562 review r1 M4, ruling R2): 20 days held on the I/O tab (30-day reach), then
    /// the Wait Stats tab (7 days). The tab reads its own 7 days, ending where the range ends, and says so in its label and in a
    /// notice on the page. The range stays held, so the I/O tab takes all of it again. A 30-day pick is refused on the tab that
    /// cannot read it, with the tab's reason, and no read on that tab asks for more than 168 hours.
    /// </summary>
    [Fact]
    public void ARangeCarriedToATabThatReadsLess_IsReadAtThatTabsReach_AndSaysSo()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("tabReachCarried", out var r, CatalogJson())) return;

        var found = r.GetProperty("found");
        Assert.Equal(JsonValueKind.Null, found.GetProperty("holdTwentyDays").ValueKind);
        Assert.Equal(480, found.GetProperty("ioTwentyDays").GetProperty("hours").GetInt32());
        Assert.Equal(JsonValueKind.Null, found.GetProperty("ioTwentyDays").GetProperty("note").ValueKind);

        var waits = found.GetProperty("waitsTwentyDays");
        Assert.Equal(168, waits.GetProperty("hours").GetInt32());
        Assert.StartsWith("last 7 days of ", waits.GetProperty("label").GetString(), System.StringComparison.Ordinal);
        var note = waits.GetProperty("note").GetString()!;
        Assert.Contains("longer than this tab reads", note, System.StringComparison.Ordinal);
        Assert.Contains("up to 7 days", note, System.StringComparison.Ordinal);

        /* The picker's own refusal on the tab that cannot read 30 days says why. */
        Assert.Equal("This tab reads up to 7 days.", found.GetProperty("holdThirtyOnWaits").GetString());
        Assert.Contains("up to 7 days", found.GetProperty("holdThirtyOnWaitsRange").GetProperty("error").GetString());

        /* On the I/O tab 30 days is held whole; carried to Wait Stats it is read as 7 days, with the notice on the page. */
        Assert.Equal(JsonValueKind.Null, found.GetProperty("holdThirtyOnIo").ValueKind);
        Assert.Equal(720, found.GetProperty("ioThirty").GetProperty("hours").GetInt32());
        Assert.Equal(JsonValueKind.Null, found.GetProperty("ioThirty").GetProperty("note").ValueKind);
        var thirty = found.GetProperty("waitsThirty");
        Assert.Equal(168, thirty.GetProperty("hours").GetInt32());
        Assert.Contains("(last 30 days) is longer than this tab reads", thirty.GetProperty("note").GetString(), System.StringComparison.Ordinal);
        Assert.Contains(thirty.GetProperty("note").GetString()!, Strings(r, "notices"));

        var reads = Strings(found, "waitsReads");
        Assert.NotEmpty(reads);
        Assert.All(reads, read => Assert.True(HoursAsked(read) <= McpHelpers.MaxHoursBack, "The Wait Stats tab asked for more than it reads: " + read));
        Assert.Empty(Strings(r, "errors"));
    }

    /// <summary>A wide range held for one server, then a server whose catalog the page has not read: the panels wait for the catalog and
    /// read once at the tab's reach (720 hours on the I/O tab), instead of reading the common 168 hours first and the rest after.</summary>
    [Fact]
    public void AWideRange_OnAServerWhoseCatalogIsNotRead_WaitsForTheCatalog_AndReadsOnce()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("tabReachWaits", out var r, CatalogJson())) return;

        var found = r.GetProperty("found");
        var fetches = Strings(found, "fetches");
        var reads = fetches.Where(f => f.StartsWith("/api/read/", System.StringComparison.Ordinal)).ToArray();
        var windowed = reads.Where(read => HoursAsked(read) > 0).ToArray();
        Assert.NotEmpty(windowed);
        Assert.All(windowed, read => Assert.Equal(720, HoursAsked(read)));
        Assert.InRange(found.GetProperty("catalog").GetInt32(), 0, found.GetProperty("firstRead").GetInt32() - 1);
        Assert.Empty(Strings(r, "notices"));
        Assert.Empty(Strings(r, "errors"));
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
        var subHour = found.GetProperty("subHour");
        Assert.Equal(1, subHour.GetProperty("hours").GetInt32());
        Assert.Equal("2026-01-02T10:30:00.000Z", subHour.GetProperty("asOf").GetString());
        Assert.Contains("shortest range is 5 minutes", found.GetProperty("tooShort").GetProperty("error").GetString());
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
        Assert.Contains("/api/read/get_top_queries_by_cpu?server=SRV1&hours=4&top=20&detail=full&as_of=2026-01-02T10%3A30%3A00.000Z", reads);
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
        /* An entry runs from its `["name"] = R(` to the next entry's start (or the end of the file), so one that wraps
           over several lines is read whole. */
        var starts = Regex.Matches(catalog, @"\[""(\w+)""\] = R\(").ToArray();
        var entries = starts.Select((m, i) => (Name: m.Groups[1].Value, Body: catalog[m.Index..(i + 1 < starts.Length ? starts[i + 1].Index : catalog.Length)])).ToArray();
        Assert.True(entries.Length > 100, "The read catalog's entries were not found.");
        var windowedWithoutAsOf = entries
            .Where(e => e.Body.Contains("PHours(", System.StringComparison.Ordinal) && !e.Body.Contains("PAsOf()", System.StringComparison.Ordinal))
            .Select(e => e.Name)
            .OrderBy(n => n, System.StringComparer.Ordinal)
            .ToArray();
        Assert.NotEmpty(windowedWithoutAsOf);

        var util = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "util.js");
        var set = Regex.Match(util, @"READS_WITHOUT_AS_OF = new Set\(\[(.*?)\]\)", RegexOptions.Singleline);
        Assert.True(set.Success, "util.js no longer declares READS_WITHOUT_AS_OF.");
        var declared = Regex.Matches(set.Groups[1].Value, @"""(\w+)""").Select(m => m.Groups[1].Value).OrderBy(n => n, System.StringComparer.Ordinal).ToArray();
        Assert.Equal(windowedWithoutAsOf, declared);
    }

    /// <summary>A read wider than the store keeps is asked again for the hours it keeps. On a custom range that retry keeps the
    /// range's end (<c>as_of</c>), the chart draws only the part of the range the store holds, and the notice says so.</summary>
    [Fact]
    public void ANarrowedRead_OnACustomRange_KeepsTheEndAndSaysItShowsThePartTheStoreHolds()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("customKeptHistory", out var r)) return;

        var fetches = Strings(r, "fetches");
        Assert.Contains("/api/read/get_cpu_utilization?server=SRV1&hours=24&as_of=2026-01-02T10%3A30%3A00.000Z", fetches);
        Assert.Contains("/api/read/get_cpu_utilization?server=SRV1&hours=4&as_of=2026-01-02T10%3A30%3A00.000Z", fetches);
        Assert.DoesNotContain("/api/read/get_cpu_utilization?server=SRV1&hours=4", fetches.Where(f => !f.Contains("as_of=", System.StringComparison.Ordinal)));
        /* 06:30 to 10:30 is the part of 07:15-yesterday to 10:30 a 4-hour store holds: 07:30, 09:00 and 10:00 are in, 06:00 is out. */
        Assert.Equal(new[] { 3 }, r.GetProperty("chartPoints").EnumerateArray().Select(e => e.GetInt32()).ToArray());
        Assert.Contains("the part of the custom range the store still holds", Assert.Single(Strings(r, "notices")));
    }

    /// <summary>The trim cuts per-point series. A row that stamps its LAST sample (the latch read's <c>captured_at</c>) is a total over
    /// the window and stays, even when that stamp falls in the rounded-up slack before the picked start.</summary>
    [Fact]
    public void TheTrim_LeavesAggregateRowsAlone()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("customAggregateRows", out var r)) return;

        var found = r.GetProperty("found");
        Assert.Equal(2, found.GetProperty("latchRows").GetInt32());
        Assert.Equal(3, found.GetProperty("seriesRows").GetInt32());
    }

    /// <summary>A span that is not a whole number of hours says where totals and rankings begin; a whole-hour span adds nothing.</summary>
    [Fact]
    public void ARoundedRange_SaysWhereTheAggregatesBegin()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("customRoundedLabel", out var r)) return;

        var found = r.GetProperty("found");
        Assert.Contains("aggregate from", found.GetProperty("rounded").GetString());
        Assert.DoesNotContain("aggregate from", found.GetProperty("whole").GetString());
    }
}
