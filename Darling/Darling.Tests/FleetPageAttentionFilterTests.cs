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
using System.Runtime.CompilerServices;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2437 / #2424: the web dashboard's "+N more need attention" stops being a dead end.
///
/// <para><b>What was reported.</b> On a 57-server fleet the fleet page rendered "+52 more need attention" as an
/// inert muted div — no handler, no navigation, nothing. @ehaar's question on the desktop twin was literally
/// "where do I find these warnings?", and the honest answer on this surface was the same: nowhere. The 52 are
/// not missing data; <c>BuildRollup</c> computes the whole problem set and then truncates it for display, so the
/// ranking beyond <c>DefaultWorstCount</c> is discarded rather than unavailable.</para>
///
/// <para><b>What is pinned here.</b> The two properties #2429 settled on, ported. First, the browser filters on
/// the SAME predicate the count was computed from — the card's server-computed <c>band</c>, not a client-side
/// re-derivation — so the destination cannot disagree with the label that sent you there. That half is pinned
/// against BOTH artifacts: the reduction's own arithmetic runs here, and the literal the browser compares
/// against is built from the shipped serializer rather than typed out. Second, the active state is visible and
/// clearable, so a filtered grid can never be mistaken for an unfiltered one.</para>
///
/// <para>The wiring half text-scans the shipped <c>fleet.js</c> (located by walking up from this file's
/// compile-time path, the <see cref="ViewerGridPayloadColumnOrderPinTests"/> pattern) because this repository
/// carries no JavaScript test runner and a handler in a browser module is out of reach of any assertion about a
/// C# object. Behaviour was verified separately by running that same file under a DOM shim; see the PR.</para>
/// </summary>
public sealed class FleetPageAttentionFilterTests
{
    /// <summary>The shipped module, newlines normalised so a multi-line anchor holds whether the checkout gave
    /// this file CRLF (.gitattributes says it does) or LF.</summary>
    private static string FleetJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "fleet.js"));

    /// <summary>
    /// The count and the filter are one predicate. <c>BuildRollup</c> derives "+N more" from
    /// <c>Band != Healthy</c>, the payload carries that band per card, and the browser compares against the
    /// token the SHIPPED serializer emits for <see cref="FleetHealthBand.Healthy"/> — so the string in the JS is
    /// pinned to the enum rather than transcribed from it. Renaming the band member breaks this test instead of
    /// silently emptying the grid the link lands on.
    /// </summary>
    [Fact]
    public void TheBrowsersFilter_ReadsTheSameBandTheCountWasComputedFrom()
    {
        var cards = BuildFleet(healthy: 45, critical: 4, warning: 6, offline: 2);
        var rollup = DarlingFleetReader.BuildRollup(cards, DateTime.UtcNow, DateTime.UtcNow.AddHours(-1), DateTime.UtcNow);

        /* The server's own arithmetic: everything not Healthy is either ranked or counted in the overflow. */
        var problems = cards.Count(c => c.Band != FleetHealthBand.Healthy);
        Assert.Equal(12, problems);
        Assert.Equal(problems, rollup.WorstServers.Count + rollup.AdditionalProblemCount);
        Assert.Equal(problems, rollup.WarningCount + rollup.CriticalCount + rollup.OfflineCount);

        /* And the browser's predicate compares against the band this payload actually carries. */
        var healthyToken = JsonSerializer.Serialize(FleetHealthBand.Healthy, DarlingFleetReader.JsonOptions);
        Assert.Equal("\"Healthy\"", healthyToken);
        Assert.Contains("return c.band !== " + healthyToken + ";", FleetJs, StringComparison.Ordinal);

        /* Read off the card, never recomputed from the metrics — the R1 rule the whole page is built on. */
        Assert.DoesNotContain("cpu_severity ===", FleetJs, StringComparison.Ordinal);
    }

    /// <summary>The ranking cap is what creates the overflow, so the number the link carries is the number the
    /// reduction produced — not a second opinion assembled in the browser.</summary>
    [Fact]
    public void TheOverflowCount_IsTheServersOwn()
    {
        var rollup = DarlingFleetReader.BuildRollup(
            BuildFleet(healthy: 5, critical: 40, warning: 12, offline: 0),
            DateTime.UtcNow, DateTime.UtcNow.AddHours(-1), DateTime.UtcNow);

        Assert.Equal(5, rollup.WorstServers.Count);
        Assert.Equal(47, rollup.AdditionalProblemCount);
        Assert.Contains("\"+ \" + d.additional_problem_count + \" more need attention\"", FleetJs, StringComparison.Ordinal);
    }

    /// <summary>
    /// The line navigates, and can be reached without a mouse. <c>onActivate</c> rather than <c>onClick</c> is
    /// the load-bearing choice: it is util.el's path that also installs <c>role="button"</c>, a tabindex and
    /// Enter/Space, which is what every other clickable div on this page already gets. A plain click handler
    /// would look identical on screen and be invisible to a keyboard.
    /// </summary>
    [Fact]
    public void TheOverflowLine_IsActivatable_AndTurnsTheFilterOn()
    {
        var js = FleetJs;
        var at = js.IndexOf("if (d.additional_problem_count > 0) {", StringComparison.Ordinal);
        Assert.True(at > 0, "the '+N more need attention' affordance is gone — find where it moved before editing this test");

        var end = js.IndexOf("\n    }", at, StringComparison.Ordinal);
        Assert.True(end > at, "could not find the end of the overflow-line block");
        var block = js[at..end];

        Assert.Contains("onActivate: () => setAttentionOnly(true)", block, StringComparison.Ordinal);

        /* The class it USED to carry. A muted div is the shape of a caption, and that is exactly how a reader
           treated it: as a number reporting that 52 servers exist somewhere, not as the way to them. */
        Assert.DoesNotContain("class: \"muted\"", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// The filter is clearable and says what it did. A filtered grid that looks unfiltered is a worse defect
    /// than the dead end it replaces, and the all-clear case — the filter on with nothing left to show — is the
    /// one that would otherwise read as a broken page. The wording is the desktop viewer's
    /// <c>FleetRollup.AttentionFilterCountText</c> verbatim, because the reason all three surfaces are being
    /// fixed together is that a reader moving between them should meet one vocabulary.
    /// </summary>
    [Fact]
    public void TheFilter_SaysWhatItDid_InTheSameWordsTheViewerUses()
    {
        Assert.Contains("\"showing \" + shown + \" of \" + total", FleetJs, StringComparison.Ordinal);
        Assert.Contains("\"all \" + total + \" servers are healthy\"", FleetJs, StringComparison.Ordinal);
        Assert.Contains("\"the 1 server monitored is healthy\"", FleetJs, StringComparison.Ordinal);
        Assert.Contains("\"no servers to filter\"", FleetJs, StringComparison.Ordinal);

        /* The one place this surface needs words the viewer does not have, and review found out why: the
           viewer's Overview has no search box, so "the 1 server monitored is healthy" is simply true there.
           Here the denominator is what the SEARCH left, so on a 57-server fleet narrowed to one match that
           sentence claims the fleet holds one server while 56 others exist and were never looked at. An
           all-clear has to name the population it is clearing. */
        Assert.Contains("\"the 1 matching server is healthy\"", FleetJs, StringComparison.Ordinal);
        Assert.Contains("\"all \" + total + \" matching servers are healthy\"", FleetJs, StringComparison.Ordinal);
        Assert.Contains("attentionCountText(shown, total, term !== \"\")", FleetJs, StringComparison.Ordinal);

        var viewer = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.Fleet.cs"));
        Assert.Contains("$\"showing {shown} of {total}\"", viewer, StringComparison.Ordinal);
        Assert.Contains("$\"all {total} servers are healthy\"", viewer, StringComparison.Ordinal);
        Assert.Contains("\"the 1 server monitored is healthy\"", viewer, StringComparison.Ordinal);
        Assert.Contains("\"no servers to filter\"", viewer, StringComparison.Ordinal);
    }

    /// <summary>
    /// The colour follows the SENTENCE, not the filter. This line says either "N servers need attention" or an
    /// all-clear, and painting the all-clear amber would be a colour contradicting its own text — the family of
    /// defect the whole change is about. Raised in review on #2429 for the viewer's count; it costs one ternary
    /// here and the same mistake was available.
    /// </summary>
    [Fact]
    public void TheActiveState_IsPaintedByWhatItSays_NotByBeingOn()
    {
        /* THREE sentences, not two. Review caught the third: a search term that matched nothing leaves the
           filter with nothing to judge, and the green all-clear arm fired there — a colour asserting that the
           fleet is fine when its problem servers were never looked at, which is the exact defect in miniature. */
        Assert.Contains("const searchFoundNothing = term !== \"\" && total === 0;", FleetJs, StringComparison.Ordinal);
        Assert.Contains("searchFoundNothing ? \"none\" : shown > 0 ? \"warn\" : \"ok\"", FleetJs, StringComparison.Ordinal);
        Assert.Contains("nothing matches that term, so no server was judged.", FleetJs, StringComparison.Ordinal);

        /* The notice re-words itself with no page load, so it is announced rather than silently swapped —
           util.js's noticeStrip idiom for the same kind of non-fatal live notice. Also raised in review. */
        Assert.Contains("role: \"status\"", FleetJs, StringComparison.Ordinal);

        var css = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "css", "app.css"));
        Assert.Contains(".attention-note.warn", css, StringComparison.Ordinal);
        Assert.Contains(".attention-note.ok", css, StringComparison.Ordinal);
        Assert.Contains(".attention-note.none", css, StringComparison.Ordinal);

        /* And the affordances the JS names actually have rules — a class with no stylesheet behind it renders
           as body text, which is precisely the inert muted div this replaces. */
        Assert.Contains(".attention-link", css, StringComparison.Ordinal);
        Assert.Contains(".attention-control", css, StringComparison.Ordinal);
        Assert.Contains(".attention-link:focus-visible", css, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2772: the fleet card grid stretches its cards to fill the row, rather than shrinking them as the window
    /// widens.
    ///
    /// <para>`.grid` (the fleet server cards, and the Custom Views list's `.view-cards`) sized its tracks with
    /// <c>auto-fill</c>, which creates as many 260px tracks as the viewport holds whether or not there are cards
    /// for them. With fewer cards than tracks, the cards occupied the first tracks and shrank into them while the
    /// rest sat empty — so a WIDER window made the cards NARROWER, until a card fell below what its inner
    /// three-tile <c>.stats</c> row needs and the third tile clipped mid-text (<c>BP 4.0 G</c>, <c>0 fai</c>).
    /// <c>auto-fit</c> collapses the empty tracks so the cards fill the row instead, the fix <c>.stats</c> itself
    /// already used one screen down for the identical reason. The bug is invisible at fleet scale — every track
    /// occupied — and obvious at two servers, so a silent revert to <c>auto-fill</c> would pass every eye and
    /// every large-fleet demo. Pinned as source because the repo carries no CSS/DOM test runner (the FleetJs
    /// scan pattern above); the fix was also verified live at a two-card width, before and after.</para>
    /// </summary>
    [Fact]
    public void FleetAndViewCardGrids_StretchWithAutoFit_NotAutoFill()
    {
        var app = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "css", "app.css"));
        /* .grid is the only 260px-track grid in app.css (.stats is 150px), so this pins it specifically. */
        Assert.Contains("repeat(auto-fit, minmax(260px, 1fr))", app, StringComparison.Ordinal);
        /* No grid track in app.css uses auto-fill (the .stats comment says the word but not "repeat(auto-fill"). */
        Assert.DoesNotContain("repeat(auto-fill", app, StringComparison.Ordinal);

        var editor = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "css", "editor.css"));
        Assert.Contains("repeat(auto-fit, minmax(260px, 1fr))", editor, StringComparison.Ordinal);
        Assert.DoesNotContain("repeat(auto-fill", editor, StringComparison.Ordinal);
    }

    /// <summary>
    /// One place turns the filter on and off, and the header checkbox is where its state lives — so the "+N
    /// more" link cannot shrink the grid behind the toggle's back, and either affordance can undo the other.
    /// The toggle is also re-seeded from module state on every render, which is what stops the 60-second
    /// refresh quietly dropping an active filter.
    /// </summary>
    [Fact]
    public void TheFilterState_HasOneHome_AndSurvivesTheRefresh()
    {
        Assert.Contains("function setAttentionOnly(on)", FleetJs, StringComparison.Ordinal);
        Assert.Contains("if (attentionToggle) attentionToggle.checked = on;", FleetJs, StringComparison.Ordinal);
        Assert.Contains("cb.checked = attentionOnly;", FleetJs, StringComparison.Ordinal);
        Assert.Contains("onActivate: () => setAttentionOnly(false)", FleetJs, StringComparison.Ordinal);

        /* The declaration and exactly one assignment — the setter. A second writer is how a link and a toggle
           end up disagreeing about whether the grid is filtered. */
        Assert.Equal(2, CountOccurrences(FleetJs, "attentionOnly = "));

        /* Deliberately NOT persisted, unlike the sort and the grouped view: a sort is a preference, this is a
           triage action tied to a moment, and a page that opens with 52 of 57 servers already hidden is a
           support ticket even with the toggle in plain sight (#2429's ruling, ported). */
        Assert.DoesNotContain("darling.fleet.attention", FleetJs, StringComparison.Ordinal);
    }

    /// <summary>
    /// The grid is filtered in ONE place, composed with the search term rather than replacing it, and both
    /// render paths (flat and tag-grouped) get the notice — a grouped fleet must not be the one view where an
    /// active filter is invisible.
    /// </summary>
    [Fact]
    public void TheGrid_IsFilteredOnce_AndBothViewsShowTheActiveState()
    {
        Assert.Contains("const searched = lastCards.filter((c) => cardMatches(c, fleetFilter));", FleetJs, StringComparison.Ordinal);
        Assert.Contains("attentionOnly ? searched.filter(cardNeedsAttention) : searched", FleetJs, StringComparison.Ordinal);

        /* One call site: the grouped view and the flat view are projected from the SAME filtered list, so a
           tag-grouped fleet cannot end up with its own opinion about what needs attention. */
        Assert.Equal(1, CountOccurrences(FleetJs, "searched.filter(cardNeedsAttention)"));

        /* And the notice's denominator is what the SEARCH left, not the fleet. With a term typed, "showing 4 of
           57" invites reading 4 as the fleet's problem count; the other 53 were never looked at. */
        Assert.Contains("attentionNotice(matched.length, searched.length)", FleetJs, StringComparison.Ordinal);

        Assert.Contains("mount(gridNode, [notice, renderGrouped(matched)]);", FleetJs, StringComparison.Ordinal);

        /* One sentence per state. The notice already explains an empty grid whenever it is showing, and in
           more precise words, so the grid-area fallback is suppressed under it rather than stacking a second
           box that says the same thing differently — which is the PR's own design goal losing to itself, and
           a deviation from the desktop viewer, which shows only its count (raised in review). What is left is
           the case the notice does not cover: the filter off, the search term the only thing that emptied it. */
        Assert.Contains(": notice ? null : el(\"div\", { class: \"muted\"", FleetJs, StringComparison.Ordinal);
        Assert.Contains("return term ? \"No servers match", FleetJs, StringComparison.Ordinal);
        Assert.DoesNotContain("need attention.\";", FleetJs, StringComparison.Ordinal);
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────

    /// <summary>A fleet of pre-banded cards — the shape <c>BuildCard</c> produces, which is the only input
    /// <c>BuildRollup</c> reads. The bands are set directly because the reduction bands nothing itself; it
    /// counts what the shared classifier already decided.</summary>
    private static List<FleetServerCard> BuildFleet(int healthy, int critical, int warning, int offline)
    {
        var fleet = new List<FleetServerCard>();
        var id = 0;

        void Add(int count, FleetHealthBand band, string prefix)
        {
            for (var i = 0; i < count; i++)
            {
                fleet.Add(new FleetServerCard
                {
                    ServerId = ++id,
                    DisplayName = $"{prefix}{i:00}",
                    ServerName = $"{prefix}{i:00}",
                    Band = band,
                    IsOnline = band != FleetHealthBand.Offline,
                });
            }
        }

        Add(healthy, FleetHealthBand.Healthy, "h");
        Add(critical, FleetHealthBand.Critical, "c");
        Add(warning, FleetHealthBand.Warning, "w");
        Add(offline, FleetHealthBand.Offline, "o");

        return fleet;
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }
        return count;
    }

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate).Replace("\r\n", "\n", StringComparison.Ordinal);
            }
        }

        throw new FileNotFoundException($"Could not locate {relative} walking up from {thisFile}");
    }
}
