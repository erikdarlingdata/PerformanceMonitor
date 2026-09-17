/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The graphical index-bloat grids REACH the answered indexes, and the prose beside them describes the rows
/// they are showing (#3434).
///
/// <para><b>The defect.</b> Answerless rows sort FIRST by design (#3278), so a grid that fetches one capped
/// page in the read's own order shows 100% answerless rows whenever that population outnumbers the cap —
/// structurally rather than by chance, and at EVERY cap while the leading block is larger. The desktop
/// viewer caps at 200 and the web dashboard at 25. Measured on the fleet at 2026-09-14 through the
/// monitoring store's own census: the largest target holds 2,505 candidate btree indexes of which 1,779 are
/// answerless and 726 carry a trusted answer covering 213.8 GB, and the next holds 268 answerless ahead of
/// 79 answers covering 191.0 GB. Both leading blocks exceed 200, so both grids showed no answer at all
/// beside a coverage note correctly reporting that hundreds exist.</para>
///
/// <para><b>Why the fixture has to exceed the cap.</b> A population whose answerless rows FIT inside the
/// grid cannot tell a reachable answer from an unreachable one — the default page already contains answers
/// there, so every assertion passes before the fix and after it. So each reach pin states the defect
/// numerically from its own fixture first (<c>Assert.Equal(0, ...)</c> over the default page's answered
/// rows) and only then composes. That line is what makes the pin's regime the one the bug lives in, and it
/// is the line a shrunken fixture fails on rather than passing quietly.</para>
///
/// <para><b>Why reach is asserted as a NUMBER.</b> "The answered read was performed" is satisfied by a read
/// whose every row is then discarded by the composition. Every pin here counts the rows it ends up with.</para>
///
/// <para><b>The two halves are asserted TOGETHER.</b>
/// <see cref="TheProseAndTheRowSetDescribeOnePopulation"/> builds the grid and the sentence from one call and
/// requires every numeral in the sentence to be a recount of the grid — a pin checking the count in one test
/// and the prose in another cannot see them disagree, which is this defect arriving one layer in.</para>
///
/// <para><b>What cannot be verified here.</b> The desktop viewer is WPF and cannot be run or screenshotted
/// on the hosts this work happens on, so the RENDERED grid is not asserted anywhere. What is asserted is the
/// composition, its prose, and — by source scan — that the panel takes its every figure off the composed
/// page rather than off either read. <c>Lite</c> has no index-bloat panel at all: the PostgreSQL collectors
/// are Darling-only, so there is no cross-SKU parity question on this surface.</para>
/// </summary>
public sealed class PgIndexBloatGridReachTests
{
    private const string ViewerPanel =
        "Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.Postgres.cs";

    private const string WebTabs =
        "Darling/PerformanceMonitor.Darling.Service/wwwroot/js/pages/server-tabs.js";

    /// <summary>The measured census this whole issue is about: answerless rows, then answered ones.</summary>
    private const int FleetAnswerless = 1_779;

    private const int FleetAnswered = 726;

    /// <summary>
    /// The viewer's own row cap, READ OUT OF THE VIEWER rather than retyped.
    ///
    /// <para>A literal copy here would be a second declaration of the cap, and the pins below decide the
    /// difference between reachable and unreachable against it — so a cap changed in the panel and not here
    /// would leave every assertion below answering confidently about a grid that does not exist. Asserted to
    /// match exactly once, so a rename fails loudly instead of defaulting.</para>
    /// </summary>
    private static int ViewerRowCap()
    {
        var matches = Regex.Matches(
            RepoFile.ReadRepoFile(ViewerPanel), @"PgGridRowLimit\s*=\s*(?<cap>[0-9_]+)\s*;");

        var only = Assert.Single(matches);

        return int.Parse(only.Groups["cap"].Value.Replace("_", string.Empty), CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// The caps the pins sweep: both shipped ones, their neighbours, and the degenerate small ones where an
    /// arithmetic floor is the only thing that keeps an answer reachable.
    /// </summary>
    public static TheoryData<int> RowCaps =>
        new() { 1, 2, 3, 4, 5, 8, 24, 25, 26, 50, 199, 200, 201, 1_000 };

    /// <summary>
    /// One row. Answered rows carry a descending reclaimable figure so the fixture arrives in the order the
    /// read's own <c>ORDER BY</c> produces; answerless rows carry a reason and no figure, which is the only
    /// trust predicate on this collector.
    /// </summary>
    private static DarlingPgIndexBloatReader.PgIndexBloatRow Row(int ordinal, bool answered, int rank) =>
        new(
            DatabaseName: "appdb",
            SchemaName: "public",
            TableName: "t" + ordinal.ToString(CultureInfo.InvariantCulture),
            IndexName: "ix" + ordinal.ToString(CultureInfo.InvariantCulture),
            IndexBytes: 1_048_576L + ordinal,
            TreeLevel: 2,
            EmptyPages: 0,
            DeletedPages: 0,
            AvgLeafDensity: null,
            LeafFragmentation: null,
            EstimatedReclaimableBytes: answered ? 1_000L * rank : null,
            SkippedReason: answered ? null : "partial index: the model scales the parent reltuples",
            MeasurementKind: "estimated",
            EstBloatPct: answered ? 12.5 : null,
            IndexPages: 128,
            TableRows: 4_096,
            Fillfactor: 90,
            EstTupleBytes: 40,
            EstLeafPages: 96,
            PgstattupleAvailable: false,
            CaptureTime: new DateTime(2026, 9, 14, 0, 0, 0, DateTimeKind.Utc));

    /// <summary>
    /// The TWO reads a panel makes, shaped exactly as the shipped SQL shapes them: the default page is the
    /// answerless population followed by the answered ranking and cut at the cap, and the
    /// <c>answered_only</c> page is that ranking alone, also cut at the cap.
    ///
    /// <para>Built from one answered list so the default page's answered tail really is a prefix of the
    /// answered page — which is the property that lets the composition take the answerless block from one
    /// read and the whole answered block from the other without comparing an index identity.</para>
    /// </summary>
    private static (List<DarlingPgIndexBloatReader.PgIndexBloatRow> Leading,
                    List<DarlingPgIndexBloatReader.PgIndexBloatRow> Answered) Reads(
        int answerlessPopulation, int answeredPopulation, int cap)
    {
        var answered = Enumerable.Range(0, answeredPopulation)
            .Select(i => Row(i, answered: true, rank: answeredPopulation - i))
            .ToList();

        var answerless = Enumerable.Range(0, answerlessPopulation)
            .Select(i => Row(1_000_000 + i, answered: false, rank: 0))
            .ToList();

        return (answerless.Concat(answered).Take(cap).ToList(), answered.Take(cap).ToList());
    }

    private static PgIndexBloatGridPage<DarlingPgIndexBloatReader.PgIndexBloatRow> Compose(
        List<DarlingPgIndexBloatReader.PgIndexBloatRow> leading,
        List<DarlingPgIndexBloatReader.PgIndexBloatRow> answered,
        int cap) =>
        PgIndexBloatGridBudget.Compose(
            leading, answered, r => r.SkippedReason is not null, r => r.EstimatedReclaimableBytes ?? 0L, cap);

    /// <summary>
    /// Acceptance, half one: on the measured census the grid reaches answered rows, and how many is a
    /// number rather than "some".
    /// </summary>
    [Fact]
    public void TheGridReachesTheAnswers_OnTheFleetsLargestCensus()
    {
        var cap = ViewerRowCap();
        var (leading, answered) = Reads(FleetAnswerless, FleetAnswered, cap);

        /* THE DEFECT, from this fixture, in numbers: the default page is full and holds not one answer.
           Without this line a fixture whose answerless population fits inside the cap would pass every
           assertion below before the fix as well as after it. */
        Assert.Equal(cap, leading.Count);
        Assert.Equal(0, leading.Count(r => r.SkippedReason is null));
        Assert.True(FleetAnswerless > cap, "the fixture no longer exceeds the grid cap");

        var page = Compose(leading, answered, cap);
        var reserve = PgIndexBloatGridBudget.AnsweredReserveFor(cap);

        Assert.True(page.ReachesAnAnswer);
        Assert.Equal(reserve, page.AnsweredShown);
        Assert.Equal(cap - reserve, page.AnswerlessShown);
        Assert.Equal(cap, page.Rows.Count);

        /* Counted off the ROWS, not read off the page's own fields: the fields are what the prose prints,
           so a pin trusting them to describe the rows asserts the composition against itself. */
        Assert.Equal(reserve, page.Rows.Count(r => r.SkippedReason is null));
        Assert.Equal(cap - reserve, page.Rows.Count(r => r.SkippedReason is not null));
    }

    /// <summary>
    /// Reach is a property of the arithmetic at EVERY cap, not a consequence of 200 being a large number.
    /// The answerless population is one row larger than the cap in each case, which is the regime where a
    /// single page reaches nothing.
    /// </summary>
    [Theory]
    [MemberData(nameof(RowCaps))]
    public void AnAnsweredIndexIsReachableAtEveryRowCap(int cap)
    {
        var (leading, answered) = Reads(cap + 1, FleetAnswered, cap);

        Assert.Equal(cap, leading.Count);
        Assert.Equal(0, leading.Count(r => r.SkippedReason is null));

        var page = Compose(leading, answered, cap);

        Assert.True(
            page.AnsweredShown >= 1,
            $"a grid of {cap} row(s) reaches no answer, which is the defect at a smaller cap");
        Assert.True(page.ReachesAnAnswer);
        Assert.Equal(cap, page.Rows.Count);
        Assert.Contains(page.Rows, r => r.SkippedReason is null);
    }

    /// <summary>
    /// REACH, NOT ORDER. The answerless rows still lead and still hold the larger share of the grid, so the
    /// gaps in a census lead rather than hide — #3278's decision, which this change is not permitted to
    /// re-open.
    ///
    /// <para>The order assertion is what a count-preserving permutation of the grid fails: reversing
    /// <c>Rows</c> leaves every count in this file intact and puts the answered rows first.</para>
    ///
    /// <para>A one-row grid is excluded from the share half and only from it: one row cannot hold a member
    /// of both populations, and the reserve resolves that tie toward the answer. Neither shipped surface is
    /// within two orders of magnitude of that cap.</para>
    /// </summary>
    [Theory]
    [MemberData(nameof(RowCaps))]
    public void TheAnswerlessRowsStillLead(int cap)
    {
        var (leading, answered) = Reads(cap + 1, FleetAnswered, cap);
        var page = Compose(leading, answered, cap);

        Assert.All(page.Rows.Take(page.AnswerlessShown), r => Assert.NotNull(r.SkippedReason));
        Assert.All(page.Rows.Skip(page.AnswerlessShown), r => Assert.Null(r.SkippedReason));

        if (cap > 1)
        {
            Assert.True(
                page.AnswerlessShown >= page.AnsweredShown,
                $"a grid of {cap} row(s) gives the answered ranking {page.AnsweredShown} of them against "
                + $"{page.AnswerlessShown} answerless, so the gaps no longer lead");
        }
    }

    /// <summary>
    /// No regression where the defect never was. A server whose answerless population leaves the reserve
    /// free gets exactly the grid it got before: the same rows, in the same order, in the same numbers.
    ///
    /// <para>This is the pin a fix that applied the reserve unconditionally would fail — trading 40 answered
    /// rows for 40 answerless ones on servers that were already showing both.</para>
    /// </summary>
    [Fact]
    public void AGridWithRoomForBothIsTheUnmodifiedRead()
    {
        var cap = ViewerRowCap();
        var reserve = PgIndexBloatGridBudget.AnsweredReserveFor(cap);

        foreach (var answerlessPopulation in new[] { 0, 1, 10, cap - reserve })
        {
            var (leading, answered) = Reads(answerlessPopulation, FleetAnswered, cap);
            var page = Compose(leading, answered, cap);

            Assert.Equal(leading.Count, page.Rows.Count);
            Assert.Equal(answerlessPopulation, page.AnswerlessShown);
            Assert.Equal(cap - answerlessPopulation, page.AnsweredShown);
            Assert.False(page.TheLeadWasTruncatedForTheAnswers);

            /* ROW FOR ROW the default page, by index name - the composition is inert here rather than
               merely arriving at the same counts. */
            Assert.Equal(leading.Select(r => r.IndexName), page.Rows.Select(r => r.IndexName));
        }
    }

    /// <summary>
    /// A server where NOTHING has an answer gives the whole grid to the gaps, and the prose does not offer
    /// an explanation for a trade that did not happen. The reserve is a floor under answers that exist, not
    /// a hole punched in the grid.
    /// </summary>
    [Fact]
    public void AServerWithNoAnswerAtAllGivesTheWholeGridToTheGaps()
    {
        var cap = ViewerRowCap();
        var (leading, answered) = Reads(FleetAnswerless, 0, cap);

        Assert.Empty(answered);

        var page = Compose(leading, answered, cap);

        Assert.Equal(cap, page.AnswerlessShown);
        Assert.Equal(0, page.AnsweredShown);
        Assert.False(page.ReachesAnAnswer);
        Assert.False(page.TheLeadWasTruncatedForTheAnswers);
        Assert.DoesNotContain(
            "reserved for the answered ranking", page.Composition, StringComparison.Ordinal);
    }

    /// <summary>
    /// The two blocks are disjoint because of the PREDICATE and not because the caller filtered correctly.
    /// Handed the default page as both arguments — a caller who forgot <c>answered_only</c> — no index
    /// appears twice, and the answered rows that page held still come through.
    /// </summary>
    [Fact]
    public void NoIndexAppearsTwiceEvenWhenBothReadsAreTheSamePage()
    {
        var cap = ViewerRowCap();
        var (leading, _) = Reads(10, FleetAnswered, cap);

        var page = Compose(leading, leading, cap);

        Assert.Equal(page.Rows.Count, page.Rows.Select(r => r.IndexName).Distinct(StringComparer.Ordinal).Count());
        Assert.Equal(10, page.AnswerlessShown);
        Assert.Equal(cap - 10, page.AnsweredShown);
    }

    /// <summary>
    /// <b>Acceptance, half two, and the halves are asserted together.</b> One call produces the grid and the
    /// sentence; the rows are recounted from scratch and every numeral in the sentence must be one of those
    /// recounts.
    ///
    /// <para>The numeral sweep is the half that catches a STALE figure. A <c>Contains</c> over the two
    /// counts would pass while the sentence also carried a third number describing a population the grid is
    /// not showing — which is exactly the shape of the defect being fixed, one layer in. So the assertion is
    /// closed rather than open: extract every number the prose states and require each to be a figure the
    /// grid justifies.</para>
    ///
    /// <para>The ranked total is summed over the same rows for the same reason: it is the figure the panel
    /// prints as "bytes look reclaimable across N index(es)", so the sum and the N have to come off one row
    /// set or the sentence describes two.</para>
    /// </summary>
    [Fact]
    public void TheProseAndTheRowSetDescribeOnePopulation()
    {
        var cap = ViewerRowCap();
        var shapes = new[]
        {
            (Answerless: FleetAnswerless, Answered: FleetAnswered),
            (Answerless: 268, Answered: 79),
            (Answerless: cap + 1, Answered: 1),
            (Answerless: cap, Answered: FleetAnswered),
            (Answerless: 10, Answered: FleetAnswered),
            (Answerless: 0, Answered: FleetAnswered),
            (Answerless: FleetAnswerless, Answered: 0),
            (Answerless: 0, Answered: 0),
        };

        foreach (var (answerlessPopulation, answeredPopulation) in shapes)
        {
            var (leading, answered) = Reads(answerlessPopulation, answeredPopulation, cap);
            var page = Compose(leading, answered, cap);

            var answerlessInGrid = page.Rows.Count(r => r.SkippedReason is not null);
            var answeredInGrid = page.Rows.Count(r => r.SkippedReason is null);
            var where = $"answerless={answerlessPopulation} answered={answeredPopulation} cap={cap}";

            Assert.Equal(answerlessInGrid + answeredInGrid, page.Rows.Count);
            Assert.Equal(answerlessInGrid, page.AnswerlessShown);
            Assert.Equal(answeredInGrid, page.AnsweredShown);
            Assert.Equal(
                page.Rows.Sum(r => r.EstimatedReclaimableBytes ?? 0L), page.ReclaimableBytesShown);

            var prose = page.Composition;

            Assert.Contains(
                N(answerlessInGrid) + " index(es) with NO answer", prose, StringComparison.Ordinal);
            Assert.Contains(
                N(answeredInGrid) + " answered index(es) behind them", prose, StringComparison.Ordinal);

            /* CLOSED: no other number may appear. Each permitted figure is either a count of the grid, a
               count of a READ that the sentence labels as one, or the cap the grid was given.

               THE RESERVE SHARE IS NOT IN HERE, and its absence is the point. Permitting it
               unconditionally let the sentence state the theoretical quarter beside a cut that did not
               match it - at a 200-row grid with one answered index, "50 of the 200 row(s) are reserved"
               beside a cut at 199 - because an allowed numeral is never checked for CONSISTENCY with the
               others. A figure that is not a count of the grid has no business in a sentence about the
               grid, so the set says so rather than the assertions working around it. */
            var permitted = new HashSet<string>(StringComparer.Ordinal)
            {
                N(answerlessInGrid),
                N(answeredInGrid),
                N(answerlessInGrid + page.AnswerlessNotShown),
                N(page.AnsweredNotShown),
                N(cap),
            };

            foreach (var numeral in Regex.Matches(prose, "[0-9][0-9,]*").Select(m => m.Value))
            {
                Assert.True(
                    permitted.Contains(numeral),
                    $"the grid's own sentence states {numeral}, which is not a count of the grid it "
                    + $"describes ({where}): " + prose);
            }

            /* And a read-level figure is never stated unlabelled: the only two are introduced by the
               phrase that says whose count they are. */
            if (page.AnswerlessNotShown > 0 || page.AnsweredNotShown > 0)
            {
                Assert.Contains("this read returned", prose, StringComparison.Ordinal);
            }

            /* THE CUT SENTENCE'S OWN ARITHMETIC RECONCILES. A closed numeral set says every figure is
               permitted; it cannot say the permitted figures add up, and a sentence whose numbers do not
               add up is this defect one level in - the reader does the arithmetic the prose invites and
               gets a contradiction. So where the lead was cut, the two figures that explain the cut are
               asserted to satisfy the identity the sentence states, and the sentence is asserted to
               state THOSE figures. */
            if (page.TheLeadWasTruncatedForTheAnswers)
            {
                Assert.Equal(cap, page.AnswerlessShown + page.AnsweredShown);
                Assert.Contains(
                    "took " + N(answeredInGrid) + " of the " + N(cap) + " row(s)",
                    prose,
                    StringComparison.Ordinal);
                Assert.Contains(
                    "CUT at " + N(answerlessInGrid) + " of the ", prose, StringComparison.Ordinal);
            }
        }
    }

    /// <summary>
    /// The panel counts NOTHING of its own: the figures it prints come off the composed page, the coverage
    /// census is handed the composed row count, and the two reads are only ever arguments to the composition.
    ///
    /// <para>A source scan, because the viewer is WPF and cannot be run here. It is the narrowest claim that
    /// covers the risk this issue is about: a figure derived from either read rather than from the grid is a
    /// note describing a population the grid is not showing.</para>
    /// </summary>
    [Fact]
    public void TheViewerPanelTakesEveryFigureFromTheComposedPage()
    {
        var body = MemberBody(ViewerPanel, "LoadPgIndexBloatAsync");

        Assert.Contains("PgIndexBloatGridBudget.Compose", body, StringComparison.Ordinal);
        Assert.Contains("answeredOnly: true", body, StringComparison.Ordinal);
        Assert.Contains("var rows = page.Rows", body, StringComparison.Ordinal);
        Assert.Contains("page.ReclaimableBytesShown", body, StringComparison.Ordinal);
        Assert.Contains("page.Composition", body, StringComparison.Ordinal);

        /* The census's "rows returned" figure is the composed count, which is what is on screen. */
        Assert.Contains("_server.ServerId, endUtc, rows.Count", body, StringComparison.Ordinal);

        /* Each read is named exactly three times - assigned, joined, and handed to Compose. A fourth
           mention is a figure being derived from one read instead of from the grid. */
        Assert.Equal(3, Occurrences(body, "leadingPageTask"));
        Assert.Equal(3, Occurrences(body, "answeredPageTask"));

        /* CONCURRENT UNDER A DECLARED WIDTH, and the declaration is the load-bearing half. The two reads
           are independent, so issuing them in sequence costs this panel two full round trips where it
           used to pay one - but issuing them together WITHOUT a scope is worse than either, because
           ViewerCommandDeadlines prices each command off the width the context declares and would hand
           two contending reads a deadline computed for one. Released at the join so the census read below
           is not priced against reads that have finished. */
        Assert.Contains("ViewerReadFanOut.Of(2)", body, StringComparison.Ordinal);
        Assert.Contains("Task.WhenAll(leadingPageTask, answeredPageTask)", body, StringComparison.Ordinal);
        Assert.Contains("readFanOut.Release()", body, StringComparison.Ordinal);

        var join = body.IndexOf("Task.WhenAll", StringComparison.Ordinal);
        var release = body.IndexOf("readFanOut.Release()", StringComparison.Ordinal);
        var census = body.IndexOf("GetPgIndexBloatCoverageAsync", StringComparison.Ordinal);

        Assert.True(join < release, "the width is released before the reads it describes are joined");
        Assert.True(release < census, "the census read is still priced against two finished reads");
    }

    /// <summary>
    /// The web dashboard — the third surface, and the ONLY graphical one on a headless install — asks for the
    /// answered population too, and the panel that keeps #3278's order does not.
    /// </summary>
    [Fact]
    public void TheWebDashboardAsksForTheAnsweredPopulationInItsOwnPanel()
    {
        /* LF-normalised, because the descriptor boundary below is a multi-line anchor and an anchor
           carrying the wrong newline matches nothing and reads as clean. */
        var tabs = RepoFile.ReadRepoFileLf(WebTabs);

        var ranking = tabs.IndexOf("\"Index Bloat (answered ranking)\"", StringComparison.Ordinal);

        Assert.True(ranking > 0, "the web dashboard has no answered-ranking panel");

        var panel = tabs[ranking..Math.Min(tabs.Length, ranking + 1_200)];

        Assert.Contains("\"get_pg_index_bloat\"", panel, StringComparison.Ordinal);
        Assert.Contains("answered_only: true", panel, StringComparison.Ordinal);

        /* Its own census, from the read, over its own rows - the same hook #3278 gave the panel above. */
        Assert.Contains("\"note\"", panel, StringComparison.Ordinal);

        /* AND THE DEFAULT PANEL IS UNTOUCHED. Reach, not order: the panel that leads with the gaps must not
           have acquired the filter, or this change hid what #3278 exists to show.

           Scoped to that panel's OWN descriptor - its title through the close of its table() call - and not
           to the text between the two titles, which contains the design comment explaining the filter and
           would satisfy this assertion by accident. */
        var estimated = tabs.IndexOf("\"Index Bloat (estimated)\"", StringComparison.Ordinal);

        Assert.True(estimated > 0, "the default index bloat panel is gone or renamed");
        Assert.True(estimated < ranking, "the answered ranking is rendered ahead of the default order");

        var closed = tabs.IndexOf("\n      ),", estimated, StringComparison.Ordinal);

        Assert.True(
            closed > estimated && closed < ranking,
            "the default panel's descriptor no longer closes before the answered-ranking panel begins - "
            + "remap this pin rather than widening it");

        var defaultPanel = tabs[estimated..closed];

        Assert.Contains("\"get_pg_index_bloat\"", defaultPanel, StringComparison.Ordinal);
        Assert.DoesNotContain("answered_only", defaultPanel, StringComparison.Ordinal);

    }

    /// <summary>
    /// The extension inventory is the same shape of capped census on the same page, and it now carries both
    /// halves: the read's own caveat over the rows, and the aggregated answer that caveat points at.
    ///
    /// <para><b>Both halves, because the note alone would be the defect inverted.</b> The read's caveat says
    /// to read <c>install_census</c>; a page printing that sentence while rendering no census tells an
    /// operator to consult something the surface does not offer. The rows are the PRODUCT of databases and
    /// extension names and the ordering takes the CREATED rows first, so the row list is what a cap breaks
    /// and the census is what no cap touches (#3425).</para>
    ///
    /// <para><b>And the hook is asserted to be WIRED, not merely configured.</b> <c>noteKey</c> on a fanout
    /// spec was inert before this change — a spec carrying it would have looked right and rendered nothing,
    /// which is correct narration over absent behaviour.</para>
    /// </summary>
    [Fact]
    public void TheWebDashboardRendersTheLimitIndependentExtensionCensus()
    {
        var tabs = RepoFile.ReadRepoFileLf(WebTabs);

        /* ONE fetch feeding both panels - fanout's whole purpose, and what keeps this off the
           duplicate-fetch rule that the index bloat pair needed a named exemption for. */
        var fetch = tabs.IndexOf("fanout(\"get_pg_extensions\"", StringComparison.Ordinal);

        Assert.True(fetch > 0, "the extension inventory is no longer one fetch feeding its panels");

        var block = tabs[fetch..Math.Min(tabs.Length, fetch + 2_000)];

        Assert.Contains("title: \"Extensions\"", block, StringComparison.Ordinal);
        Assert.Contains("noteKey: \"note\"", block, StringComparison.Ordinal);
        Assert.Contains("rowsKey: \"install_census\"", block, StringComparison.Ordinal);

        /* The mechanism behind that noteKey, in fanout's own body. */
        var composite = tabs.IndexOf("function fanout(read, params, specs)", StringComparison.Ordinal);

        Assert.True(composite > 0, "fanout is gone - remap this pin rather than deleting it");

        var fanout = tabs[composite..Math.Min(tabs.Length, composite + 2_000)];

        Assert.Contains("getPath(res.data, spec.noteKey)", fanout, StringComparison.Ordinal);
        Assert.Contains("noticeStrip(note)", fanout, StringComparison.Ordinal);
    }

    /// <summary>
    /// The reserve is a SHARE of whatever cap it is given, so the same promise holds at 25 rows and at 200,
    /// and it never rounds to nothing.
    /// </summary>
    [Theory]
    [MemberData(nameof(RowCaps))]
    public void TheReserveIsAShareOfTheCapAndNeverRoundsToZero(int cap)
    {
        var reserve = PgIndexBloatGridBudget.AnsweredReserveFor(cap);

        Assert.True(reserve >= 1, $"a grid of {cap} row(s) reserves nothing for the answers");
        Assert.True(reserve <= cap, $"a grid of {cap} row(s) reserves {reserve} of them");
        Assert.Equal(Math.Max(1, cap / PgIndexBloatGridBudget.AnsweredReserveDivisor), reserve);

        /* An empty grid reserves nothing, which is the one case where zero is the answer. */
        Assert.Equal(0, PgIndexBloatGridBudget.AnsweredReserveFor(0));
        Assert.Equal(0, PgIndexBloatGridBudget.AnsweredReserveFor(-1));
    }

    private static string N(long value) => value.ToString("N0", CultureInfo.InvariantCulture);

    private static int Occurrences(string text, string needle) =>
        text.Split(needle, StringSplitOptions.None).Length - 1;

    /// <summary>
    /// One member's body with comments and literals blanked, through the shared walker rather than a regex
    /// of this file's own — a hand-rolled scan over C# is how two earlier pins counted different wrong
    /// things.
    /// </summary>
    private static string MemberBody(string relativePath, string member)
    {
        var map = CSharpMemberMap.Of(RepoFile.ReadRepoFile(relativePath));

        var declarations = map.Declarations
            .Where(d => d.Kind == CSharpMemberMap.DeclarationKind.Member
                        && string.Equals(d.Name, member, StringComparison.Ordinal))
            .ToList();

        var declaration = Assert.Single(declarations);

        Assert.True(declaration.End > declaration.Start, member + "'s body never closed");

        return map.Code[declaration.Start..declaration.End];
    }
}
