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
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// That <see cref="RepoFile"/> is the only repo-file reader in <c>Darling.Tests</c> — asserted, rather than
/// stated in prose and left to rot. <c>Darling.Tests</c> and not "the repository": the scope is this one
/// test project, and the paragraph on what the scan cannot see says what that leaves out.
///
/// <para><b>This exists because the prose version failed for thirty-four consecutive classes.</b> Every one
/// of them wanted to read a repo file, none of them found an authority to call, each wrote its own, and the
/// set drifted into EIGHT different resolution semantics — probing for the target versus finding a marker,
/// walking from the pin's source path versus from the test binary's directory, three different markers, and
/// two different answers about newlines. Nothing failed while that happened, because a private helper is
/// invisible to everything except the class holding it. The same failure mode #2913 removed from the source
/// walk and #2938 from the deadline judgement, and the same fix: one authority, and a build that reds when
/// a second one appears.</para>
///
/// <para><b>The LF-adopting set is pinned separately, and it is the half that would fail silently.</b> A
/// missing declaration is a compile error; a pin switched from <see cref="RepoFile.ReadRepoFileLf"/> to
/// <see cref="RepoFile.ReadRepoFile"/> is not. This checkout is CRLF, so an anchor spanning a line break
/// cannot match raw text at all: a positive assertion would fail loudly, but a
/// <c>DoesNotContain</c> would stop being ABLE to fire and report clean forever. Switching a pin off the LF
/// reader is therefore a decision, and it reds here so it gets made on purpose.</para>
///
/// <para><b>What this scan cannot see, stated rather than implied.</b> Four things, and the first is a
/// boundary rather than a limitation.</para>
///
/// <para><b>It sweeps <c>Darling.Tests</c> only</b>, because <see cref="TestDirectory"/> is this file's own
/// directory. <c>Lite.Tests</c> carries six private <c>ReadRepoFile</c> declarations of its own —
/// <c>DetachedCollectorGateTests</c>, <c>GridPayloadColumnOrderPinTests</c>,
/// <c>LiteLogLevelGateTests</c>, <c>LiteOverviewCardExplainsItselfTests</c>,
/// <c>LiteSidebarDotRendersTheCardStatusTests</c> and <c>QueryStoreServerGateTests</c> — and this says
/// nothing about them. That count is prose rather than an assertion, so it goes stale silently; a seventh
/// arriving is exactly the drift the sibling pin below would catch if it existed. They are the same pattern in the
/// sibling project, and <see cref="RepoFile"/> is shareable with it by the one <c>Compile Include</c> line
/// that already brings <see cref="CSharpSourceWalker"/> across. Whoever takes that has to widen this sweep
/// or add a sibling pin in <c>Lite.Tests</c>; a reader consolidated there while this scan still looks at one
/// directory would be consolidated with nothing asserting it stayed that way.</para>
///
/// <para><b>It matches a DECLARATION by NAME</b> — <c>ReadRepoFile</c> or <c>ReadRepoFileLf</c> — so a
/// private re-implementation under some other name evades it. The regex anchors on the name, and a name is
/// all one file's text offers.</para>
///
/// <para><b>It matches a CALL by NAME AND RECEIVER</b> — the bare name, or a receiver whose last segment is
/// <c>RepoFile</c>. An aliased receiver (<c>using RF = RepoFile;</c>) and a call reached through a delegate
/// captured from the method are both outside it. <b>The failure direction is why this is worth stating:</b>
/// the raw and LF lists are decided by subtracting one count from the other, so a call matching neither
/// pattern makes a file absent from BOTH lists rather than present in the wrong one — and an exact-set
/// equality is satisfied by a file it cannot see just as well as by a file that belongs outside it. That is
/// not hypothetical: while the receiver was excluded rather than consumed,
/// <see cref="RepoFileResolutionEquivalenceTests"/> called the LF reader twice and sat outside the LF
/// equality, which is the correct answer arrived at by not looking. It is now declared in
/// <see cref="s_lfSubjects"/>, and the exemption is asserted to name a file the census can see.</para>
///
/// <para><b>It says nothing about the fifty-three classes here that carry their own repo-ROOT walk</b>
/// without a reader on top of it. That is the same duplication one layer down, it is a larger population
/// than this one was, and it is deliberately not in scope.</para>
/// </summary>
public sealed class RepoFileAdoptionTests
{
    private const string Authority = "RepoFile.cs";

    /// <summary>A method DECLARATION of the shared reader's name: an accessibility modifier, no <c>;</c> or
    /// <c>=</c> before the name, then the name and its parameter list. Matched against STRIPPED source, so
    /// the doc comments in this family that quote the reader by name are not mistaken for declarations of
    /// it.</summary>
    private static readonly Regex Declaration = new(
        @"^[ \t]*(?:private|internal|public|protected)[^\r\n;=]*?\bReadRepoFile(?:Lf)?\s*\(",
        RegexOptions.Multiline | RegexOptions.Compiled);

    /// <summary>Every CALL to either reader, counted so the LF calls can be subtracted out below. The
    /// receiver group is what makes a qualified call visible, and it has to be part of the MATCH rather than
    /// permitted by the lookbehind: the lookbehind's job is excluding an unrelated
    /// <c>Something.ReadRepoFile(</c>, and it does that by refusing a preceding <c>.</c>. Consuming our own
    /// receiver moves the match start to before the dot, so the call is inside the match instead of behind
    /// it, and every other receiver stays out.</summary>
    private static readonly Regex AnyCall = new(
        @"(?<![\w.])(?:[\w.]*\bRepoFile\.)?ReadRepoFile(?:Lf)?\s*\(", RegexOptions.Compiled);

    /// <summary>The LF spelling alone, same receiver rule. <c>(?:Lf)?</c> above is what makes it a subset
    /// rather than a disjoint pattern — the trailing <c>\s*\(</c> would otherwise separate the two names on
    /// its own, and the count comparison below would then be comparing populations that never overlap.</summary>
    private static readonly Regex LfCall = new(
        @"(?<![\w.])(?:[\w.]*\bRepoFile\.)?ReadRepoFileLf\s*\(", RegexOptions.Compiled);

    /// <summary>
    /// The pins whose anchors span a line break, and which therefore read LF-normalised text.
    ///
    /// <para>Membership is a property of a pin's ANCHORS, not of its subject: a pin belongs here exactly
    /// when one of its anchors crosses a newline. The list is compared against the tree rather than trusted,
    /// for the reason in this class's summary — moving a pin between the two readers changes whether its
    /// multi-line assertions can fire at all. Consolidation found six of the thirty-four carrying the
    /// normalising helper and twenty-eight deliberately not; that was a census of one moment, and this is
    /// the live set, so it carries no count of its own to go stale.</para>
    /// </summary>
    private static readonly string[] s_lfReaders =
    {
        /* #4194: its live-mount-guard pin anchors the `if (live …) { … return; }` block in renderAlerts across
           its four source lines as one literal, so the guard's return is told apart from just those words
           appearing somewhere else in the file. */
        "AlertHistoryLazyRenderTests.cs",
        /* #4368: its innerHTML-ban pin anchors renderAlertHeaderCell's own function body across the lines
           between its declaration and the next top-level function, so the ban is scoped to real code and
           not tripped by the file's own prose comment saying "never innerHTML". */
        "AlertNotebookRenderClientTests.cs",
        "BuiltinAlertPersistenceRungTests.cs",
        "ChartWindowDomainTests.cs",
        "DarlingPathFilterGateTests.cs",
        /* #3653 (A8e, PostgreSQL host): its per-arm fire-site pin anchors `Severity: null,` on the line break
           that follows it, so the CODE spelling is told apart from the same words inside the arms' own
           comments, which continue on the same line. */
        "DarlingPgOperationalAlertTests.cs",
        "FleetCardCollectionStaleNamesItsPopulationTests.cs",
        /* #3735: its source pin anchors the collection-health statement's tail across three line breaks
           (`FROM v_collection_log` / `WHERE` / `AND` / `GROUP BY`) and the CreateCommand line on the
           CommandTimeout line that follows it — the two facts the memo promised not to touch, asserted as
           the exact shape they were in, which a single-line anchor could not tell from a rewrite. */
        "FleetCollectionHealthMemoTests.cs",
        /* #3856: its tool and reader anchors span line breaks (the nowUtc-then-memoized-call pair, the
           CreateCommand-plus-CommandTimeout shape, and the statement's own WHERE/AND tail) — the same
           reasoning as the fleet twin above, which is the file this one was written beside. */
        "PerServerCollectionHealthMemoTests.cs",
        "FleetPageAttentionFilterTests.cs",
        /* #3653 item 12 (Q6): its cross-SKU pin slices the multi-line `keyed` CTE out of the Darling provider's
           SOURCE (from `keyed AS (` to `FROM clean` + `)` on the next line) and asserts Lite's source carries the
           identical text; the clock-read twin pin likewise compares a multi-line const body one token apart. */
        "LocalClockBucketKeyTests.cs",
        "LockedModeRestoreCoverageTests.cs",
        /* #3541 A10: its top-level-key discriminator anchors on the line break BEFORE the key (a per-row
           collection_time inside a Select is indented deeper and must not match), and its tool-body slicing
           keys on attribute text either side of one. */
        "McpLatestSnapshotStampTests.cs",
        /* #3541 A3: its Lite-description anchor spans the line break between `Description(` and the
           string on get_plan_corrections, and its tool-body slicing keys on attribute text either side of
           one. */
        "McpPageContractTests.cs",
        "NightlyVersionInjectionTests.cs",
        "PgCpuCapacityHeadroomTests.cs",
        "PgIndexBloatGridReachTests.cs",
        "PgLoggingCollectorOffTests.cs",
        /* #3691 (V138): its V102-restatement pin anchors the generated column tail across two line breaks
           (`short_desc text,` / `database_name text,` / `role_name text`), its tool pin anchors the override
           section's null else-arm on the key that follows it on the next line, and its rung-doc and
           reader-census arms slice LF-normalised source. */
        /* #4004 review, round 3: its rotation-note pin anchors the collection-log write in RunOneAsync on the
           call and its argument list on the NEXT line, so the note is proven applied before THAT row is written. */
        "PgLogHashKeyTests.cs",
        "PgServerConfigScopeRungTests.cs",
        /* Reads the lineage comment blocks above the flipped PostgreSQL bars; a block is several lines and
           is collapsed to one before its citation is matched, so the read has to be LF-normalised first. */
        "PgTargetMeasuredLineageTests.cs",
        /* #3797: its viewer-surface pins slice regions out of ViewerServerTab.xaml and server-tabs.js between
           anchors several lines apart (the Queries group's sub-tab block, a column array, one DataGrid's
           column list), and its dispatch pin anchors `case QueryStoreClutterSubTabIndex:` on the awaited
           loader call on the NEXT line — anchors that a CRLF read would miss on every arm. */
        "QueryStoreClutterViewerSurfacesTests.cs",
        /* #4231: its web source pin counts noteKey: "truncation_note" occurrences across server-tabs.js and
           view-templates.js, which an LF-normalised read makes exact instead of CRLF-fragile. */
        /* #4427: its wiring pin slices RunPurgeNowAsync's body by brace balance and matches calls that span line breaks. */
        "RawPurgeNowWiringTests.cs",
        "RawWindowFloorViewerPortTests.cs",
        /* #3653 item 3: its twin pin extracts the multi-line GetPriorOccurrencesSql const body from BOTH finding
           stores' sources and asserts them equal up to one token, and its wiring pin slices each pass method by
           call sites that sit on their own lines — anchors spanning line breaks on every arm. */
        "RecurrenceLabelStoreReadTests.cs",
        "ServerPageTabsTests.cs",
        "StartupFailureTriageTests.cs",
        "StoreCopyPhaseTests.cs",
        /* #3754: its reconcile and worker pins anchor multi-statement arms across their line breaks - the
           `if (enabled) { attempted++; }` and `if (enabled) { failed++; … }` blocks in DarlingXeSessions,
           and the pre-dispatch `if (…LongQueryTraceFault is { } traceFault) { throw … }` in RunOneAsync - so
           the CODE shape is told apart from the same words inside the comments beside it. */
        "SwallowedItemFailureTests.cs",
        "ViewTemplatesTests.cs",
        "ViewerSidebarDotRendersTheCardStatusTests.cs",
    };

    /// <summary>
    /// The files that call the LF reader without ANCHORING on it, and are therefore outside the set above
    /// while being inside the census.
    ///
    /// <para>The distinction is the one <see cref="s_lfReaders"/> is keyed on: membership there is a property
    /// of a pin's anchors. A file that calls the LF reader to EXERCISE it has no anchors spanning a line
    /// break, so moving it between the two readers changes nothing about what its assertions can match, and
    /// declaring it would state a property it does not have.</para>
    ///
    /// <para><b>Declared rather than left to fall out of the scan.</b> The one entry here was outside the
    /// equality before the census could see a qualified receiver — the right answer produced by a scan that
    /// could not see the file at all, which is indistinguishable from a scan that considered it. So each
    /// entry is asserted to be VISIBLE to the census in
    /// <see cref="TheLfReadingPins_AreExactlyTheOnesDeclaredHere"/>: an exemption for a file the scan cannot
    /// find is an exemption doing no work, and it reds rather than reading as a decision.</para>
    /// </summary>
    private static readonly string[] s_lfSubjects =
    {
        /* #4198 (lane W2): its source-text pin asserts a single-line entry in the endpoint dispatch
           table (the `get_active_queries` row in DarlingWebEndpoints.cs) — the pin contains no line
           break, so LF normalization does not affect what the assertion can match. */
        "DarlingWebEndpointsTests.cs",
        /* The equivalence test FOR the reader, parameterised over both spellings and calling each of them
           directly. It compares the reader's output against the bytes on disk put through the same
           transform, so it performs the CRLF-to-LF normalisation itself rather than depending on the
           reader's — which is the property membership above is about. */
        "RepoFileResolutionEquivalenceTests.cs",
    };

    [Fact]
    public void ExactlyOneFile_DeclaresTheSharedRepoFileReader()
    {
        var (files, declarers, rawAdopters, lfAdopters) = Survey();

        /* A floor rather than an equality, and the same one the rest of this family uses: it catches a sweep
           that read the wrong directory and then reported clean on nothing. The claims that matter are the
           set equalities, which move whenever a declaration or an adopter does. */
        Assert.True(
            files.Count >= 400,
            $"the sweep found only {files.Count} .cs files under {TestDirectory()} — it is not reading the "
          + "test project, so every assertion below would pass for a reason unrelated to the defect");

        Assert.Equal(new[] { Authority }, declarers.ToArray());

        /* The authority being alone is satisfied just as well by an authority nothing calls, which is why
           the adopting population is floored on its own. Thirty-four classes were migrated onto it; a floor
           under that catches the reader being quietly abandoned while this file keeps passing. */
        var adopters = rawAdopters.Concat(lfAdopters).Distinct(StringComparer.Ordinal).ToArray();

        Assert.True(
            adopters.Length >= 30,
            $"only {adopters.Length} classes call the shared reader. Thirty-four were migrated onto it, so "
          + "either pins were deleted or they have gone back to reading files some other way — in which "
          + "case the single-declaration assertion above is guarding an authority nobody uses");
    }

    [Fact]
    public void TheLfReadingPins_AreExactlyTheOnesDeclaredHere()
    {
        var (_, _, _, lfAdopters) = Survey();

        /* Asserted BEFORE the equality, because it is the failure the equality cannot report. A file the
           census cannot see is absent from lfAdopters and absent from the expectation, so both sides agree
           and the equality passes — while the exemption that was supposed to be a decision about a visible
           file is instead a coincidence about an invisible one. */
        foreach (var subject in s_lfSubjects)
        {
            Assert.True(
                lfAdopters.Contains(subject, StringComparer.Ordinal),
                $"{subject} is exempted from the LF-reading set, but the census does not see it calling the "
              + "LF reader. Either the call is gone, in which case delete the exemption — or the scan cannot "
              + "see the spelling it uses, in which case this exemption is not the decision it claims to be "
              + "and neither is the equality below");
        }

        /* Disjoint, so the concatenation below is a set. A file in both lists would duplicate in the
           expectation and red the equality anyway, but on a length mismatch rather than on the mistake. */
        Assert.Empty(s_lfReaders.Intersect(s_lfSubjects, StringComparer.Ordinal));

        Assert.Equal(
            s_lfReaders.Concat(s_lfSubjects).OrderBy(f => f, StringComparer.Ordinal).ToArray(),
            lfAdopters.ToArray());

        /* The floor that makes the equality above mean something. An empty tree satisfies an empty
           expectation, and this list is short enough that a scan reading nothing looks plausible. */
        Assert.NotEmpty(s_lfReaders);
    }

    private static (List<string> Files, List<string> Declarers, List<string> RawAdopters, List<string> LfAdopters)
        Survey()
    {
        var directory = TestDirectory();

        /* bin/ and obj/ excluded the way CrossAppGuardCiGateTests excludes them: obj/ carries generated
           sources, and a throwaway verification harness staged under bin/ would otherwise be swept as if it
           were a pin. AllDirectories rather than TopDirectoryOnly so a pin that lands in a future
           subdirectory is still counted — the flat layout today is not an invariant anything asserts. */
        var files = Directory
            .EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories)
            .Where(f =>
                !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal) &&
                !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();

        var declarers = new List<string>();
        var rawAdopters = new List<string>();
        var lfAdopters = new List<string>();

        foreach (var file in files)
        {
            var name = Path.GetFileName(file);

            /* STRIPPED, for the reason CommandDeadlineScannerAdoptionTests gives: this family discusses the
               shared reader at length in doc comments and names both methods, so read raw, every file in it
               would look like an adopter and several like declarers. */
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));

            if (Declaration.IsMatch(code))
            {
                declarers.Add(name);
            }

            if (name.Equals(Authority, StringComparison.Ordinal))
            {
                continue;
            }

            if (LfCall.IsMatch(code))
            {
                lfAdopters.Add(name);
            }

            /* AnyCall counts the Lf spelling too, so a file is a raw adopter exactly when its calls are not
               all accounted for by the Lf pattern. Counted by occurrence rather than by presence, so a class
               that legitimately uses both readers lands in both lists instead of one arbitrarily — which is
               the difference between a subtraction and a comparison of two disjoint populations. */
            if (AnyCall.Matches(code).Count > LfCall.Matches(code).Count)
            {
                rawAdopters.Add(name);
            }
        }

        declarers.Sort(StringComparer.Ordinal);
        rawAdopters.Sort(StringComparer.Ordinal);
        lfAdopters.Sort(StringComparer.Ordinal);

        return (files, declarers, rawAdopters, lfAdopters);
    }

    private static string TestDirectory([CallerFilePath] string thisFile = "")
        => Path.GetDirectoryName(thisFile)!;
}
