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
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3154: a <c>pg_column_stats</c> row count must say WHICH of its outcomes produced it.
///
/// <para><b>The property, in one sentence.</b> Whenever a surface reports a <c>pg_column_stats</c> result,
/// the answer names which cause produced it — nothing clears the collector's page floor, the monitoring login
/// cannot see <c>pg_stats</c>, coverage is partial, or neither legitimate cause applies and it is a fault —
/// or states that it cannot tell and why; and it never presents those as one undifferentiated list.</para>
///
/// <para><b>Derived from the property's violation routes, not from reading the code.</b> Enumerating the ways
/// a zero can arise is the work here, because the population of causes IS what the guard has to discriminate:</para>
///
/// <list type="number">
/// <item>A zero with NO cause named. Shipped state: 553 SUCCESS runs across 50 targets, zero rows ever, a
/// NULL note on 99 of the last 100. Guarded by <see cref="EveryOutcomeSelectsAnArm"/>.</item>
/// <item>A cause named as PROSE that recites every arm and selects none. Also the shipped state — the read's
/// own message listed the floor and the privilege filter in one sentence, which tells a reader what it MIGHT
/// be. Guarded by <see cref="NeitherSurfaceAuthorsItsOwnMultiCauseProse"/>, which scans the two member
/// BODIES rather than their files, so the tool's own <c>[Description]</c> attribute — which legitimately
/// describes both — cannot satisfy it.</item>
/// <item>The floor arm and the privilege arm rendering the SAME thing. Guarded by
/// <see cref="NoTwoDifferentArmsRenderTheSameCause"/> — and by
/// <see cref="TheCauseCarriesNoneOfTheInputFigures"/>, without which that check would pass on the counts
/// differing rather than on the verdicts differing, since the two arms can never hold the same counts.</item>
/// <item>A COLLECTION FAULT reporting as one of the legitimate arms, which is false innocence and the route
/// by which a wrong query would never be noticed. Guarded by
/// <see cref="AReadableTableWithNothingStoredIsAFaultAndSaysSo"/>.</item>
/// <item>Insufficient evidence reported AS a diagnosis — "nothing is big enough" asserted where nothing was
/// measured. Guarded by <see cref="NoEvidenceIsNotTheSameAsNothingAboveTheFloor"/>, the pair the live store
/// confirms: the evidence collector ran 168 times and found no qualifying table on one target, and ran zero
/// times for an unknown one, and those two must not answer alike.</item>
/// <item>The verdict computed and NOT reaching the surface where the zero is seen — captured and unread, the
/// invariant #3017 turned into a rule. Guarded by <see cref="BothSurfacesPrintTheSharedVerdict"/>.</item>
/// <item>A NON-empty result hiding a partial view, so a ranking over four visible tables of twenty reads as
/// a ranking of the server. Guarded by <see cref="BothSurfacesPrintTheVerdictOnThePopulatedPathToo"/> and by
/// <see cref="PartialCoverageIsItsOwnArm"/>.</item>
/// <item>The floor an operator is TOLD about drifting from the floor the query filters on. Guarded by
/// <see cref="TheQuotedFloorIsTheOneTheShippedQueriesFilterOn"/>.</item>
/// </list>
///
/// <para><b>Which cause actually applies, measured.</b> The privilege filter, on the whole fleet. Read-only
/// against a live store on 2026-09-07: <c>estimate_unavailable</c> was TRUE on 331,357 of 331,357
/// <c>pg_table_bloat_stats</c> rows in every era, and independently the collector's own width arithmetic
/// bottomed out at its empty-input residue on 59,981 of 59,981 rows in 48 hours — two readings that fail
/// differently and agree. The floor is demonstrably NOT the explanation: 1,263 of 1,264 distinct tables past
/// the byte floor also clear the page floor, on all 49 targets that report, and the busiest holds 361 of
/// them, which is the figure the collector's own comment cites.</para>
/// </summary>
public sealed class PgColumnStatsCoverageTests
{
    /// <summary>
    /// Every arm, reached from the counts that produce it. Named cases rather than a loop so a failure says
    /// which situation regressed.
    /// </summary>
    private static readonly (string Name, PgColumnStatsCoverageVerdict Verdict, PgColumnStatsCoverageArm Expected)[] s_cases =
    {
        ("evidence collector never ran, nothing stored",
            PgColumnStatsCoverage.Classify(0, 0, 0, 0), PgColumnStatsCoverageArm.Undetermined),
        ("evidence collector never ran, rows stored anyway",
            PgColumnStatsCoverage.Classify(0, 361, 0, 1_000), PgColumnStatsCoverageArm.Undetermined),
        ("evidence read itself failed",
            PgColumnStatsCoverage.EvidenceUnreadable(0), PgColumnStatsCoverageArm.Undetermined),
        ("measured, and nothing clears the floor",
            PgColumnStatsCoverage.Classify(168, 0, 0, 0), PgColumnStatsCoverageArm.BelowSizeFloor),
        ("361 candidates, none readable - the fleet's answer",
            PgColumnStatsCoverage.Classify(167, 361, 0, 0), PgColumnStatsCoverageArm.StatisticsNotVisible),
        ("one candidate, not readable",
            PgColumnStatsCoverage.Classify(1, 1, 0, 0), PgColumnStatsCoverageArm.StatisticsNotVisible),
        ("candidates readable and nothing stored",
            PgColumnStatsCoverage.Classify(167, 361, 361, 0), PgColumnStatsCoverageArm.CollectionFault),
        ("stored, covering some of the candidates",
            PgColumnStatsCoverage.Classify(167, 361, 12, 5_000), PgColumnStatsCoverageArm.PartialVisibility),
        ("stored, covering all of them",
            PgColumnStatsCoverage.Classify(167, 361, 361, 5_000), PgColumnStatsCoverageArm.FullyMeasured),
    };

    /// <summary>R1: no outcome may arrive without an arm, and every arm must be reachable.</summary>
    [Fact]
    public void EveryOutcomeSelectsAnArm()
    {
        foreach (var (name, verdict, expected) in s_cases)
        {
            Assert.Equal(expected, verdict.Arm);
            Assert.False(string.IsNullOrWhiteSpace(verdict.Cause), name + " produced no cause");
            Assert.False(string.IsNullOrWhiteSpace(verdict.Census), name + " produced no census");
        }

        /* Both directions. A classifier that had collapsed to one arm would satisfy the loop above for
           whichever arm survived, and an arm nothing can reach is a branch no test covers. */
        Assert.Equal(
            Enum.GetValues<PgColumnStatsCoverageArm>().OrderBy(a => a).ToArray(),
            s_cases.Select(c => c.Verdict.Arm).Distinct().OrderBy(a => a).ToArray());
    }

    /// <summary>
    /// R3: different arm, different verdict. Pairwise over DIFFERING arms rather than a distinctness check
    /// over every case, because two inputs reaching the same arm are REQUIRED to render the same cause — that
    /// is what makes the text a function of the arm rather than of the numbers.
    /// </summary>
    [Fact]
    public void NoTwoDifferentArmsRenderTheSameCause()
    {
        var collisions =
            (from a in s_cases
             from b in s_cases
             where a.Verdict.Arm != b.Verdict.Arm
             && string.Equals(a.Verdict.Cause, b.Verdict.Cause, StringComparison.Ordinal)
             select a.Name + " reads the same as " + b.Name).ToList();

        Assert.Empty(collisions);

        /* Named rather than counted: two inputs whose only difference is the candidate count reach the same
           arm, and the cause has to be identical - otherwise the text is a function of the numbers. */
        Assert.Equal(
            Named("361 candidates, none readable - the fleet's answer").Cause,
            Named("one candidate, not readable").Cause);

        /* The two Undetermined situations are deliberately NOT one sentence: "the evidence collector does not
           run here" is the design working on a replica, and "the store read failed" is a monitoring fault. */
        Assert.NotEqual(s_cases[0].Verdict.Cause, s_cases[2].Verdict.Cause);
    }

    /// <summary>
    /// R3's falsifier. Without this, <see cref="NoTwoDifferentArmsRenderTheSameCause"/> is satisfied by a
    /// classifier that had stopped selecting anything and merely echoed its inputs — the arms can never hold
    /// identical counts, so their composed messages differ whatever the verdict says.
    ///
    /// <para><b>Every arm, not one.</b> The first draft of this used a single input, which landed on
    /// <see cref="PgColumnStatsCoverageArm.PartialVisibility"/>; a mutation that echoed the counts into the
    /// <see cref="PgColumnStatsCoverageArm.CollectionFault"/> cause went GREEN under it. A falsifier has to
    /// discriminate the thing being claimed, and the claim is about all of them.</para>
    /// </summary>
    [Fact]
    public void TheCauseCarriesNoneOfTheInputFigures()
    {
        var probes = new[]
        {
            PgColumnStatsCoverage.Classify(0, 424_242, 31_337, 90_210),
            PgColumnStatsCoverage.Classify(8_675_309, 0, 0, 0),
            PgColumnStatsCoverage.Classify(8_675_309, 424_242, 0, 0),
            PgColumnStatsCoverage.Classify(8_675_309, 424_242, 31_337, 0),
            PgColumnStatsCoverage.Classify(8_675_309, 424_242, 31_337, 90_210),
            PgColumnStatsCoverage.Classify(8_675_309, 424_242, 424_242, 90_210),
            PgColumnStatsCoverage.EvidenceUnreadable(90_210),
        };

        /* And the probe set really does visit every arm, or "no arm echoes its inputs" would be a claim
           about however many arms this list happens to reach. */
        Assert.Equal(
            Enum.GetValues<PgColumnStatsCoverageArm>().OrderBy(a => a).ToArray(),
            probes.Select(p => p.Arm).Distinct().OrderBy(a => a).ToArray());

        foreach (var probe in probes)
        {
            foreach (var figure in new[]
                { "8675309", "8,675,309", "424242", "424,242", "31337", "31,337", "90210", "90,210" })
            {
                Assert.DoesNotContain(figure, probe.Cause, StringComparison.Ordinal);
            }
        }

        /* And the counts are not merely absent from the cause - they are PRESENT in the census, or the
           checks above would be satisfied by a class that had stopped reporting them at all. */
        Assert.Contains("424,242", probes[4].Census, StringComparison.Ordinal);
        Assert.Contains("31,337", probes[4].Census, StringComparison.Ordinal);
        Assert.Contains("90,210", probes[4].Census, StringComparison.Ordinal);
    }

    /// <summary>R3, on the specific pair the issue is about: the two LEGITIMATE zero-causes.</summary>
    [Fact]
    public void TheTwoLegitimateZeroCausesNameThemselvesAndRuleTheOtherOut()
    {
        var floor = Case(PgColumnStatsCoverageArm.BelowSizeFloor).Cause;
        var privilege = Case(PgColumnStatsCoverageArm.StatisticsNotVisible).Cause;

        Assert.NotEqual(floor, privilege);

        Assert.Contains("size floor", floor, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("has_column_privilege", floor, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("GRANT", floor, StringComparison.Ordinal);

        Assert.Contains("not the size floor", privilege, StringComparison.Ordinal);
        Assert.Contains("has_column_privilege", privilege, StringComparison.Ordinal);
        /* The remedy is the difference between naming a cause and being actionable about it. */
        Assert.Contains("GRANT pg_read_all_data", privilege, StringComparison.Ordinal);
    }

    /// <summary>R4: a readable table plus a stored nothing is a fault and must not read as either innocent arm.</summary>
    [Fact]
    public void AReadableTableWithNothingStoredIsAFaultAndSaysSo()
    {
        var fault = PgColumnStatsCoverage.Classify(167, 361, 1, 0);

        Assert.Equal(PgColumnStatsCoverageArm.CollectionFault, fault.Arm);
        Assert.Contains("COLLECTION FAULT", fault.Cause, StringComparison.Ordinal);
        Assert.Contains("neither legitimate", fault.Cause, StringComparison.OrdinalIgnoreCase);
        /* Both innocent explanations refused BY NAME, so the sentence cannot be skimmed as either. */
        Assert.Contains("floor does not explain it", fault.Cause, StringComparison.Ordinal);
        Assert.Contains("privilege filter does not explain it", fault.Cause, StringComparison.Ordinal);
    }

    /// <summary>
    /// R5: the pair that has to differ, and the one the live store confirms — one target's evidence collector
    /// ran 168 times and found no qualifying table, and an unknown target's ran zero times. Identical counts,
    /// different answers, and the RUN count is the only thing that separates them.
    /// </summary>
    [Fact]
    public void NoEvidenceIsNotTheSameAsNothingAboveTheFloor()
    {
        var measured = PgColumnStatsCoverage.Classify(168, 0, 0, 0);
        var unmeasured = PgColumnStatsCoverage.Classify(0, 0, 0, 0);

        Assert.Equal(PgColumnStatsCoverageArm.BelowSizeFloor, measured.Arm);
        Assert.Equal(PgColumnStatsCoverageArm.Undetermined, unmeasured.Arm);
        Assert.NotEqual(measured.Cause, unmeasured.Cause);

        /* A measured zero prints as a figure; an unmeasured one prints as a WORD. Rendering both as 0 is
           how "we looked and everything is small" became indistinguishable from "we never looked". */
        Assert.Contains("floor: 0.", measured.Census, StringComparison.Ordinal);
        Assert.DoesNotContain(PgColumnStatsCoverage.NotMeasured, measured.Census, StringComparison.Ordinal);
        Assert.Contains("floor: " + PgColumnStatsCoverage.NotMeasured, unmeasured.Census, StringComparison.Ordinal);

        /* And the Undetermined arm must not smuggle a verdict in. */
        Assert.DoesNotContain("CAUSE:", unmeasured.Cause, StringComparison.Ordinal);
        Assert.Contains("cannot be established", unmeasured.Cause, StringComparison.Ordinal);
    }

    /// <summary>
    /// R5's other half: the evidence lookback must not be the caller's window, or a short panel window
    /// MANUFACTURES <see cref="PgColumnStatsCoverageArm.Undetermined"/> on a server whose answer is known.
    ///
    /// <para>The evidence collector is hourly and the subject collector is daily, so a one-hour viewer
    /// window straddles zero or one evidence run — and reporting "no evidence" for a target measured 167
    /// times in the past week is the failing-toward-a-confident-nothing direction. A wider caller window is
    /// honoured as-is; only the floor is imposed.</para>
    /// </summary>
    [Fact]
    public void TheEvidenceLookbackIsFlooredRatherThanTakenFromTheCallersWindow()
    {
        var end = new DateTime(2026, 9, 7, 20, 0, 0, DateTimeKind.Utc);
        var floorHours = DarlingPgColumnStatsReader.MinimumEvidenceHours;

        /* A one-hour read still looks back the floor. */
        Assert.Equal(
            end.AddHours(-floorHours),
            DarlingPgColumnStatsReader.EvidenceStart(end.AddHours(-1), end));

        /* A month-long read is NOT narrowed to the floor - the rows it returned come from that month, and
           narrowing would report coverage over a population the data does not come from. */
        Assert.Equal(
            end.AddDays(-30),
            DarlingPgColumnStatsReader.EvidenceStart(end.AddDays(-30), end));

        /* Anchored on the END, so an as_of read gets evidence contemporary with the data it explains
           rather than today's. */
        var earlier = end.AddDays(-10);
        Assert.Equal(
            earlier.AddHours(-floorHours),
            DarlingPgColumnStatsReader.EvidenceStart(earlier.AddHours(-1), earlier));

        /* And the floor is at least the SUBJECT collector's cadence: pg_column_stats runs daily, so
           evidence over a shorter span than one of its own cycles cannot describe the run being explained. */
        Assert.True(
            floorHours >= 24,
            $"the evidence floor is {floorHours}h, shorter than pg_column_stats' own daily cadence");
    }

    /// <summary>R7: a populated result that covers part of the target is its own answer, not the clean one.</summary>
    [Fact]
    public void PartialCoverageIsItsOwnArm()
    {
        var partial = PgColumnStatsCoverage.Classify(167, 20, 4, 900);
        var whole = PgColumnStatsCoverage.Classify(167, 20, 20, 900);

        Assert.Equal(PgColumnStatsCoverageArm.PartialVisibility, partial.Arm);
        Assert.Equal(PgColumnStatsCoverageArm.FullyMeasured, whole.Arm);
        Assert.NotEqual(partial.Cause, whole.Cause);
        Assert.Contains("PARTIAL COVERAGE", partial.Cause, StringComparison.Ordinal);
        Assert.Contains("GRANT pg_read_all_data", partial.Cause, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT", whole.Cause, StringComparison.Ordinal);
    }

    /// <summary>
    /// R8: the floor quoted at an operator is the floor the queries filter on. Read from the SHIPPED query
    /// text and the SHIPPED census SQL, not from a retyped 128 — a message promising "128 pages" over a
    /// query filtering on something else is a lie no arithmetic test would see.
    /// </summary>
    [Fact]
    public void TheQuotedFloorIsTheOneTheShippedQueriesFilterOn()
    {
        var collectorSql = PgColumnStatsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 1,
            ServerName = "server",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
        }).Text;

        var floor = PgColumnStatsCollector.MinimumRelPages.ToString(CultureInfo.InvariantCulture);

        Assert.Contains("c.relpages >= " + floor, collectorSql, StringComparison.Ordinal);

        /* Twice in the census: the candidate count and the visible count are the same predicate, and a
           census that filtered the two halves differently would report a visible count over a different
           population than the candidate count it is reported against. */
        var floorFiltersInCensus = DarlingPgColumnStatsReader.CoverageEvidenceSql
            .Split("heap_pages >= " + floor, StringSplitOptions.None).Length - 1;

        Assert.Equal(2, floorFiltersInCensus);

        /* And every arm's census quotes it, so the number an operator reads came from the same place. */
        foreach (var (name, verdict, _) in s_cases)
        {
            Assert.Contains(floor + " page floor", verdict.Census, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// R6: both surfaces print the SHARED verdict. The MCP tool and the WPF panel answer the same question
    /// for the same operator, and a fix applied to one of them is the defect this issue is a third instance
    /// of. Scanning for the shared call is what makes "they cannot disagree" a property rather than a hope.
    /// </summary>
    [Fact]
    public void BothSurfacesPrintTheSharedVerdict()
    {
        foreach (var (file, member, accessor) in Surfaces)
        {
            /* CODE only. A design comment saying "this calls the shared classifier" would satisfy a raw
               scan while the call itself had been removed - correct narration over absent behaviour. */
            var code = CSharpSourceWalker.StripCommentsAndStrings(MemberBody(file, member));

            Assert.Contains(accessor, code, StringComparison.Ordinal);
            Assert.Contains("coverage.Message", code, StringComparison.Ordinal);

            /* And neither surface may build a verdict of its own. Calling the accessor is only half the
               claim: a body that called it and then overwrote the result would pass the line above. */
            Assert.DoesNotContain("new PgColumnStatsCoverageVerdict", code, StringComparison.Ordinal);
        }

        /* The viewer reaches the classifier through its data service, so the CHAIN is what the property is
           about - pinning the panel's call alone would leave a passthrough free to author its own answer. */
        var passthrough = CSharpSourceWalker.StripCommentsAndStrings(MemberBody(
            "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.Postgres.cs",
            "GetPgColumnStatsCoverageAsync"));

        Assert.Contains("DarlingPgColumnStatsReader.GetCoverageVerdictAsync", passthrough, StringComparison.Ordinal);
    }

    /// <summary>
    /// R7 at the surface. The coverage sentence has to ride the POPULATED branch as well, or a partial view
    /// is diagnosed only in the one case where nobody needs the diagnosis. Asserted by counting the prints:
    /// one branch printing it twice would satisfy a presence check.
    /// </summary>
    [Fact]
    public void BothSurfacesPrintTheVerdictOnThePopulatedPathToo()
    {
        foreach (var (file, member, _) in Surfaces)
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(MemberBody(file, member));
            var prints = code.Split("coverage.Message", StringSplitOptions.None).Length - 1;

            Assert.True(
                prints >= 2,
                $"{member} prints the coverage verdict {prints} time(s); it has to reach the empty result AND "
                + "the populated one, or a partial view is only ever explained when it is total");
        }
    }

    /// <summary>
    /// R2: neither surface authors its own multi-cause prose. This is the check that would have failed on
    /// the shipped code — both surfaces held a literal naming the size floor AND the privilege filter in one
    /// breath, which is a description of the mechanism standing in for a diagnosis of it.
    ///
    /// <para>Scoped to the member BODY, deliberately. The tool's <c>[Description]</c> attribute names both
    /// causes and should — it describes what the tool collects — and a file-scoped scan would be satisfied by
    /// that attribute while the body went on reciting. Same reason the viewer scan is scoped to the loader
    /// rather than to a file holding twenty panels.</para>
    ///
    /// <para><b>The literals are JOINED before matching, not tested one at a time.</b> A per-literal check
    /// went green against a restored copy of the exact prose this issue is about, because an operator-facing
    /// sentence in this repo is a chain of concatenated literals wrapped at column 110 — "above a size
    /// floor, " and "a monitoring login without SELECT on a " are two literals and one sentence. Matching
    /// per literal tests the line wrapping.</para>
    /// </summary>
    [Fact]
    public void NeitherSurfaceAuthorsItsOwnMultiCauseProse()
    {
        foreach (var (file, member, _) in Surfaces)
        {
            var body = MemberBody(file, member);
            var prose = string.Join(
                string.Empty,
                CSharpSourceWalker.StringLiteralBodies(body).Select(l => l.Text));

            var namesTheFloor = prose.Contains("size floor", StringComparison.OrdinalIgnoreCase)
                || prose.Contains("1 MB floor", StringComparison.OrdinalIgnoreCase);
            var namesThePrivilegeFilter =
                prose.Contains("has_column_privilege", StringComparison.OrdinalIgnoreCase)
                || prose.Contains("SELECT privilege", StringComparison.OrdinalIgnoreCase)
                || prose.Contains("without SELECT", StringComparison.OrdinalIgnoreCase);

            Assert.False(
                namesTheFloor && namesThePrivilegeFilter,
                $"{member} authors prose reciting BOTH zero-causes and selecting neither, which is what "
                + $"#3154 is. It has a classifier for that. Its literals joined: {prose}");

            /* Neither half on its own either, in the surface's OWN words: a body that named only the
               privilege filter would still be pre-empting the classifier, and would be wrong on the
               target where the floor is the answer. */
            Assert.False(
                namesTheFloor || namesThePrivilegeFilter,
                $"{member} names a zero-cause in its own prose rather than printing the verdict: {prose}");
        }
    }

    private static PgColumnStatsCoverageVerdict Case(PgColumnStatsCoverageArm arm) =>
        s_cases.First(c => c.Verdict.Arm == arm).Verdict;

    /// <summary>
    /// The two surfaces that report this collector's results to a person, the member on each that does it,
    /// and the accessor that member has to obtain the verdict FROM.
    ///
    /// <para>A list, because both are named in the property — a scan for "everything that reads
    /// pg_column_stats" would also sweep the reader and the data-service passthrough, neither of which
    /// renders anything. The accessor differs per surface and that is not incidental: the MCP tool calls the
    /// store reader directly while the panel goes through the viewer's data service, and asserting one name
    /// for both would have to be the loosest of the two to pass.</para>
    /// </summary>
    private static IEnumerable<(string File, string Member, string Accessor)> Surfaces =>
    [
        ("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgIndexTools.cs",
            "GetPgColumnStats", "DarlingPgColumnStatsReader.GetCoverageVerdictAsync"),
        ("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.Postgres.cs",
            "LoadPgColumnStatsAsync", "_dataService.GetPgColumnStatsCoverageAsync"),
    ];

    private static PgColumnStatsCoverageVerdict Named(string name) =>
        s_cases.First(c => string.Equals(c.Name, name, StringComparison.Ordinal)).Verdict;

    /// <summary>
    /// One member's brace-balanced body, verbatim. Callers narrow it themselves — the call-presence scans
    /// strip comments and literals, the prose scan reads the literals — because the two questions need
    /// opposite halves of the same text and a single pre-stripped form would silently answer one of them
    /// against the wrong half.
    /// </summary>
    private static string MemberBody(string relativePath, string member)
    {
        var source = ReadSource(relativePath);

        /* The signature is located on a comment-and-string-stripped copy so a mention of the member name
           inside a doc comment cannot be mistaken for its declaration; the OFFSETS are then used against
           the original text, which the walker guarantees are the same because it replaces rather than
           removes.

           The DECLARATION, not the first occurrence - and this is not defensive tidiness. The viewer calls
           LoadPgColumnStatsAsync from the storage-tab loader 60 lines ABOVE declaring it, so a first-match
           scan lands on the call site, brace-balances the WRONG method, and then reports whatever that
           method happens to contain. It read a sibling panel's body and failed for a reason that had
           nothing to do with this collector. Every occurrence is classified and exactly one must be a
           declaration, so a rename or an added overload fails loudly rather than silently re-aiming. */
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(source);
        var declarations = Declarations(stripped, member).ToList();

        Assert.Equal(1, declarations.Count);

        var at = declarations[0];
        var open = stripped.IndexOf('{', at);
        var semicolon = stripped.IndexOf(';', at);

        Assert.True(open > at || semicolon > at, $"{member} in {relativePath} has no body of either shape");

        /* Expression-bodied members are in scope, not an exception to skip: the viewer's data-service
           passthrough is one, and it is the middle link of the chain this asserts. Whichever terminator
           comes first decides the shape - a `;` before any `{` is `=> expression;`, and reaching for the
           brace regardless would balance the NEXT member's block and scan the wrong code, which is the
           same failure the declaration-versus-call-site lookup above exists to prevent. */
        if (semicolon > at && (open < 0 || semicolon < open))
        {
            return source[at..(semicolon + 1)];
        }

        var extent = CSharpSourceWalker.BraceBalanced(stripped, open).Length;

        return source[open..(open + extent)];
    }

    /// <summary>
    /// Offsets in <paramref name="stripped"/> where <paramref name="member"/> is DECLARED rather than
    /// called. A declaration carries an access modifier ahead of it within its own statement; a call site
    /// carries <c>await</c>, a receiver, or nothing at all. Classifying every occurrence rather than taking
    /// the first is what makes the count assertable.
    /// </summary>
    private static IEnumerable<int> Declarations(string stripped, string member)
    {
        for (var at = stripped.IndexOf(member + "(", StringComparison.Ordinal);
             at >= 0;
             at = stripped.IndexOf(member + "(", at + 1, StringComparison.Ordinal))
        {
            /* Back to the end of the previous statement or block. Whatever sits between that and the name
               is this occurrence's own head, and only a declaration's head holds a modifier. */
            var statementStart = stripped.LastIndexOfAny([';', '{', '}'], at) + 1;
            var head = stripped[statementStart..at];

            if (head.Contains(" private ", StringComparison.Ordinal)
                || head.Contains(" public ", StringComparison.Ordinal)
                || head.Contains(" internal ", StringComparison.Ordinal)
                || head.Contains(" protected ", StringComparison.Ordinal))
            {
                yield return at;
            }
        }
    }

    private static string ReadSource(string relative)
    {
        var path = Path.Combine(RepoRoot(), relative);

        Assert.True(File.Exists(path), $"#3154 scan target not found: {path}");

        return File.ReadAllText(path);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
