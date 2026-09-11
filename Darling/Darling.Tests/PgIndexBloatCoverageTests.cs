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
/// #3278: a <c>get_pg_index_bloat</c> read must say what share of the SERVER it covers, in bytes as well as
/// in rows, from a figure no row limit can reach.
///
/// <para><b>The property, in one sentence.</b> Every figure this surface publishes is true of the population
/// it appears to describe: a page-scoped count is named as such, the population figures come from a separate
/// query the caller's limit does not touch, each of them carries BYTES beside its count, and the read either
/// names which coverage arm produced the result or states that it cannot tell and why.</para>
///
/// <para><b>Derived from the property's violation routes, not from reading the code.</b> Enumerating how a
/// truncated page can be mistaken for a coverage claim is the work here:</para>
///
/// <list type="number">
/// <item>A page of all-suppressed rows read as the server's coverage. Structurally guaranteed, not
/// probabilistic: answerless rows sort FIRST, so any limit below the answerless population returns 100% of
/// them. Four independent sessions did this in one night. Guarded by
/// <see cref="BothSurfacesPrintTheSharedVerdict"/> and <see cref="EveryOutcomeSelectsAnArm"/>.</item>
/// <item>The population figure computed and then NOT reaching the surface where the page is seen — captured
/// and unread. Guarded by <see cref="BothSurfacesPrintTheSharedVerdict"/> and
/// <see cref="BothSurfacesPrintTheVerdictOnThePopulatedPathToo"/>.</item>
/// <item>A surface calling the classifier and then AUTHORING its own verdict, which passes any presence
/// check. Guarded by the <c>DoesNotContain</c> half of <see cref="BothSurfacesPrintTheSharedVerdict"/>.</item>
/// <item>Counts published WITHOUT bytes, which ranks the suppression buckets wrongly. Not a preference: on
/// the fleet at 2026-09-10 the second largest bucket by rows is the LAST by footprint at 0.0004% of it, and
/// the fourth by rows is second by bytes at 1,385 GB. Guarded by
/// <see cref="TheSuppressionBucketsAreRankedByBytesAndNeverByRows"/> and
/// <see cref="EveryPublishedCountCarriesItsBytes"/>.</item>
/// <item>Trust keyed on <c>est_tuple_bytes</c> rather than on <c>skipped_reason IS NULL</c> — incident four,
/// which reported 461 GB covered against a real 214 of 5,954. The intermediate is populated on 100% of
/// answerless rows and both conclusions on 0% of them. Guarded by
/// <see cref="TheOnlyTrustPredicateIsTheAbsenceOfAReason"/>.</item>
/// <item>The census and the row list disagreeing about which of an index's rows is current, so the published
/// trusted count contradicts the grid printed beside it index by index. Guarded by
/// <see cref="TheCensusAndTheRowReadAgreeWhichRowIsCurrent"/>.</item>
/// <item>A double-counted denominator: <c>pg_index_bloat</c> writes every btree every cycle, so an
/// aggregate without <c>DISTINCT ON</c> reports twice the indexes and twice the footprint — wrong in the
/// direction that flatters coverage. Guarded by <see cref="TheCensusAndTheRowReadAgreeWhichRowIsCurrent"/>
/// and <see cref="TheCensusIsOnePopulationAndItsPartsAddUp"/>.</item>
/// <item>An absent or ERRORING collector answering "nothing to fix". Measured: 3 of 7 runs in a week errored
/// on a live target, one on a 300-second client-side deadline that stored nothing. Guarded by
/// <see cref="NoEvidenceIsNotTheSameAsNoIndexes"/>.</item>
/// <item>Evidence read over the CALLER's window, which manufactures a verdict from the panel's own zoom
/// level. Guarded by <see cref="TheEvidenceLookbackIsFixedRatherThanTakenFromTheCallersWindow"/>.</item>
/// <item>The reason markers drifting from the prose the collector actually writes, which sends a whole
/// bucket to <see cref="PgIndexBloatSuppression.Unrecognized"/> silently. Guarded by
/// <see cref="EveryReasonMarkerStillAppearsInTheShippedCollectorQuery"/>.</item>
/// <item>A verdict contradicting the row set printed beside it. Guarded by
/// <see cref="NoVerdictContradictsTheRowSetPrintedBesideIt"/>, which enumerates input REGIONS rather than
/// arms — reading branches finds a wrong assertion, only sweeping regions finds a missing one.</item>
/// </list>
/// </summary>
public sealed class PgIndexBloatCoverageTests
{
    private const long Kib = 1024L;
    private const long Mib = Kib * 1024L;
    private const long Gib = Mib * 1024L;

    private static readonly CultureInfo Inv = CultureInfo.InvariantCulture;

    private const string ReaderFile =
        "Darling/PerformanceMonitor.Darling.Storage/DarlingPgIndexBloatReader.cs";

    /// <summary>
    /// The fleet's own suppression shape, 2026-09-10: 50 targets, 7,597 rows, 5,970 GB of index footprint.
    /// Typed in the order the ISSUE reports them — by rows — deliberately, so a classifier that preserved
    /// input order would fail <see cref="TheSuppressionBucketsAreRankedByBytesAndNeverByRows"/> rather than
    /// pass it by accident.
    /// </summary>
    private static readonly PgIndexBloatSuppressionBucket[] FleetBuckets =
    [
        Bucket(PgIndexBloatSuppression.ColumnWidthsNotVisible, 2_954, 4_144 * Gib),
        Bucket(PgIndexBloatSuppression.ParentNeverAnalyzed, 2_225, 22 * Mib),
        Bucket(PgIndexBloatSuppression.NegativeBloat, 803, 211 * Gib),
        Bucket(PgIndexBloatSuppression.Partial, 698, 1_385 * Gib),
    ];

    /// <summary>The fleet's trusted tally over the same capture: 917 indexes holding 229 GB, 3.84%.</summary>
    private static readonly PgIndexBloatTally FleetTrusted = new(917, 229 * Gib);

    private static readonly PgIndexBloatTally Nothing = default;

    /// <summary>
    /// Every arm, reached from the figures that produce it. Named cases rather than a loop so a failure says
    /// which situation regressed.
    /// </summary>
    private static readonly (string Name, PgIndexBloatCoverageVerdict Verdict, PgIndexBloatCoverageArm Expected)[] s_cases =
    {
        ("collector recorded no succeeding run, nothing returned",
            PgIndexBloatCoverage.Classify(
                evidenceCollectorRan: false, Nothing, Nothing, [], 0),
            PgIndexBloatCoverageArm.Undetermined),
        ("collector recorded no succeeding run, rows returned anyway",
            PgIndexBloatCoverage.Classify(false, FleetTrusted, Nothing, FleetBuckets, 1_000),
            PgIndexBloatCoverageArm.Undetermined),
        ("the evidence read itself failed",
            PgIndexBloatCoverage.EvidenceUnreadable(0),
            PgIndexBloatCoverageArm.Undetermined),
        ("collector ran, server holds no btree index",
            PgIndexBloatCoverage.Classify(true, Nothing, Nothing, [], 0),
            PgIndexBloatCoverageArm.NoCandidates),
        ("the fleet's pre-grant answer: candidates, not one trusted",
            PgIndexBloatCoverage.Classify(true, Nothing, Nothing, FleetBuckets, 1_000),
            PgIndexBloatCoverageArm.NothingTrusted),
        ("the fleet as measured: 917 trusted of 7,597, 3.84% of the bytes",
            PgIndexBloatCoverage.Classify(true, FleetTrusted, Nothing, FleetBuckets, 25),
            PgIndexBloatCoverageArm.PartialCoverage),
        ("every candidate index answered",
            PgIndexBloatCoverage.Classify(true, FleetTrusted, Nothing, [], 25),
            PgIndexBloatCoverageArm.FullyTrusted),
        ("rows returned and the evidence window holds no candidate",
            PgIndexBloatCoverage.Classify(true, Nothing, Nothing, [], 25),
            PgIndexBloatCoverageArm.EvidenceStale),
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

        /* Both directions. A classifier collapsed to one arm would satisfy the loop above for whichever arm
           survived, and an arm nothing can reach is a branch no test covers. */
        Assert.Equal(
            Enum.GetValues<PgIndexBloatCoverageArm>().OrderBy(a => a).ToArray(),
            s_cases.Select(c => c.Verdict.Arm).Distinct().OrderBy(a => a).ToArray());
    }

    /// <summary>
    /// R4, the requirement this issue turns on: the suppression buckets are ranked by BYTES, and the cause
    /// names the byte-dominant one.
    ///
    /// <para>The fleet's figures are the input because the two rankings DISAGREE on them, which is what
    /// makes this pin able to fail. A census reporting counts alone would point an operator at
    /// <see cref="PgIndexBloatSuppression.ParentNeverAnalyzed"/> — second largest of five by rows, last by
    /// footprint at 22 MB of 5,970 GB — and hide the 1,385 GB one.</para>
    /// </summary>
    [Fact]
    public void TheSuppressionBucketsAreRankedByBytesAndNeverByRows()
    {
        var verdict = Case(PgIndexBloatCoverageArm.NothingTrusted);

        Assert.Equal(
            new[]
            {
                PgIndexBloatSuppression.ColumnWidthsNotVisible,
                PgIndexBloatSuppression.Partial,
                PgIndexBloatSuppression.NegativeBloat,
                PgIndexBloatSuppression.ParentNeverAnalyzed,
            },
            verdict.Suppressed.Select(b => b.Reason).ToArray());

        /* THE FALSIFIER. Without this the assertion above is satisfied by any stable ordering that happens
           to agree on this input - and the whole claim is that ordering by rows gives a DIFFERENT answer.
           If a later edit makes the two orderings agree, this pin stops being able to fail and says so. */
        Assert.NotEqual(
            FleetBuckets.OrderByDescending(b => b.IndexCount).Select(b => b.Reason).ToArray(),
            verdict.Suppressed.Select(b => b.Reason).ToArray());

        /* And the CAUSE names the byte-dominant bucket, not the row-dominant one. The remedy an operator
           acts on is attached to that name, so getting it from the wrong ranking sends them at 22 MB. */
        Assert.Contains(
            PgIndexBloatCoverage.Label(PgIndexBloatSuppression.ColumnWidthsNotVisible),
            verdict.Cause,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            PgIndexBloatCoverage.Label(PgIndexBloatSuppression.ParentNeverAnalyzed),
            verdict.Cause,
            StringComparison.Ordinal);

        /* The byte-dominant bucket here is the remediable one, so its remedy - not a structural refusal -
           is what the cause carries. */
        Assert.Contains("GRANT pg_read_all_data", verdict.Cause, StringComparison.Ordinal);

        /* PartialCoverage ranks them the same way. Two surfaces reading two orderings is the defect one
           level up; two ARMS reading two orderings is the same defect one level down. */
        Assert.Equal(
            verdict.Suppressed.Select(b => b.Reason).ToArray(),
            Case(PgIndexBloatCoverageArm.PartialCoverage).Suppressed.Select(b => b.Reason).ToArray());
    }

    /// <summary>
    /// R4's other half: every count the census publishes carries its BYTES, and a non-zero byte figure never
    /// renders as a zero.
    ///
    /// <para>The second clause is not pedantry. The fleet's buckets span 22 MB to 4,144 GB, so a census
    /// fixed on one unit prints "0.0 GB" beside 2,225 indexes — which recreates, inside the fix, the exact
    /// confusion between "none" and "a small amount" that the whole class exists to prevent.</para>
    /// </summary>
    [Fact]
    public void EveryPublishedCountCarriesItsBytes()
    {
        var verdict = Case(PgIndexBloatCoverageArm.PartialCoverage);

        /* Each bucket: its count AND its bytes, adjacent, so neither can be read without the other. */
        foreach (var bucket in verdict.Suppressed)
        {
            Assert.Contains(
                PgIndexBloatCoverage.Label(bucket.Reason) + " " + bucket.IndexCount.ToString("N0", Inv) + " / ",
                verdict.Census,
                StringComparison.Ordinal);
        }

        /* The two ends of the fleet's range, each in a unit that carries information. */
        Assert.Contains("4,144.0 GB", verdict.Census, StringComparison.Ordinal);
        Assert.Contains("22.0 MB", verdict.Census, StringComparison.Ordinal);

        /* The population totals and both trusted halves, count and bytes - the expected strings BUILT from
           the same constants the input is built from. Typing the sums by hand got both of them wrong on the
           first pass (5,971 and 5,742 against a real 5,969 and 5,740), which is the reason a pin over a
           derived figure should derive it rather than restate it. */
        var suppressedCount = FleetBuckets.Sum(b => b.IndexCount);
        var suppressedBytes = FleetBuckets.Sum(b => b.IndexBytes);

        Assert.Contains(
            Expect(FleetTrusted.IndexCount + suppressedCount, FleetTrusted.IndexBytes + suppressedBytes),
            verdict.Census,
            StringComparison.Ordinal);
        Assert.Contains(
            Expect(FleetTrusted.IndexCount, FleetTrusted.IndexBytes),
            verdict.Census,
            StringComparison.Ordinal);
        Assert.Contains(
            Expect(suppressedCount, suppressedBytes), verdict.Census, StringComparison.Ordinal);

        /* And those really are the fleet's published figures, so the derivation above is anchored to the
           measurement rather than to whatever the constants happen to say. */
        Assert.Equal(7_597, FleetTrusted.IndexCount + suppressedCount);
        Assert.Equal(6_680, suppressedCount);

        /* THE BYTE-WEIGHTED SHARE, which is the figure the row share gets wrong: 917 of 7,597 reads as 12%
           by rows and is 3.84% of the footprint. Both are asserted - the right one present and the wrong
           one absent - because a census printing the row share would look like it had answered. */
        Assert.Contains("3.84% of the candidate footprint", verdict.Census, StringComparison.Ordinal);
        Assert.DoesNotContain("12.07%", verdict.Census, StringComparison.Ordinal);

        /* And the returned row count is LABELLED as the page it is, beside figures that are not. */
        Assert.Contains(
            "Rows returned (over the window you asked for, and capped by your row limit): 25.",
            verdict.Census,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// R7: the census is ONE population and its parts add up — the candidate total is derived from the
    /// trusted tallies and the buckets rather than separately queried, so it cannot disagree with the
    /// breakdown printed to explain it.
    /// </summary>
    [Fact]
    public void TheCensusIsOnePopulationAndItsPartsAddUp()
    {
        var verdict = Case(PgIndexBloatCoverageArm.PartialCoverage);

        var suppressedCount = verdict.Suppressed.Sum(b => b.IndexCount);
        var suppressedBytes = verdict.Suppressed.Sum(b => b.IndexBytes);

        Assert.Equal(verdict.Trusted.IndexCount + suppressedCount, verdict.Candidates.IndexCount);
        Assert.Equal(verdict.Trusted.IndexBytes + suppressedBytes, verdict.Candidates.IndexBytes);

        /* Trusted is itself derived from its two halves, so a surface cannot print a trusted total that
           differs from the estimated/measured split beside it. */
        Assert.Equal(
            verdict.Estimated.IndexCount + verdict.ExactlyMeasured.IndexCount,
            verdict.Trusted.IndexCount);
        Assert.Equal(
            verdict.Estimated.IndexBytes + verdict.ExactlyMeasured.IndexBytes,
            verdict.Trusted.IndexBytes);

        /* And the fleet's own numbers arrive intact rather than being reduced to a ratio somewhere. */
        Assert.Equal(7_597, verdict.Candidates.IndexCount);
        Assert.Equal(917, verdict.Trusted.IndexCount);

        /* Merging is by KEY, so two stored prose strings mapping to one bucket become ONE entry. Two
           entries under one key would let a reader add a bucket to itself. */
        var merged = PgIndexBloatCoverage.Classify(
            true,
            Nothing,
            Nothing,
            [
                Bucket(PgIndexBloatSuppression.Partial, 10, 10 * Gib),
                Bucket(PgIndexBloatSuppression.Partial, 5, 5 * Gib),
            ],
            0);

        var single = Assert.Single(merged.Suppressed);
        Assert.Equal(15, single.IndexCount);
        Assert.Equal(15 * Gib, single.IndexBytes);
    }

    /// <summary>
    /// R5: <c>skipped_reason IS NULL</c> is the only trust predicate, at every layer that could substitute
    /// another one.
    ///
    /// <para>This is incident four's guard. <c>est_tuple_bytes</c> is populated on 100% of answerless rows
    /// in every bucket — 2954/2954, 2225/2225, 803/803, 698/698 — while <c>est_bloat_pct</c> and
    /// <c>est_reclaimable_bytes</c> are populated on 0% of them. So a census keyed on the intermediate would
    /// count every row as covered and print complete coverage over a population with 3.84%.</para>
    /// </summary>
    [Fact]
    public void TheOnlyTrustPredicateIsTheAbsenceOfAReason()
    {
        var census = DarlingPgIndexBloatReader.CoverageEvidenceSql;

        /* The two CONCLUSION columns appear nowhere in the census. Their absence is the property: a query
           that does not read them cannot be keyed on them. */
        Assert.DoesNotContain("est_bloat_pct", census, StringComparison.Ordinal);
        Assert.DoesNotContain("est_reclaimable_bytes", census, StringComparison.Ordinal);

        /* The grouping key IS the reason, so trusted and suppressed are separated by the reason and by
           nothing else. */
        Assert.Contains("GROUP BY latest.skipped_reason", census, StringComparison.Ordinal);

        /* est_tuple_bytes appears only as the PROVENANCE test, never as a filter deciding whether a row
           counts. A WHERE or FILTER on it would be the substitution this pin exists to catch. */
        Assert.Contains("est_tuple_bytes IS NOT NULL", census, StringComparison.Ordinal);
        Assert.DoesNotContain("WHERE est_tuple_bytes", census, StringComparison.Ordinal);
        Assert.DoesNotContain("FILTER (WHERE latest.est_tuple_bytes", census, StringComparison.Ordinal);

        /* AND THE MAPPING ASKS THE REASON FIRST. The reader turns census groups into tallies, and that is
           the one place the two could be transposed: every suppressed group's est_tuple_bytes is non-null,
           so a mapping that split on provenance BEFORE testing the reason would file 6,680 answerless
           indexes as estimates and publish them as coverage. Source order is the only thing that notices,
           because both orderings compile and both return a plausible number. */
        var mapping = CSharpSourceWalker.StripCommentsAndStrings(
            MemberBody(ReaderFile, "GetCoverageVerdictAsync"));

        var reasonTest = mapping.IndexOf("group.SkippedReason is null", StringComparison.Ordinal);
        var provenance = mapping.IndexOf("group.IsEstimate", StringComparison.Ordinal);

        Assert.True(reasonTest > 0, "the reader no longer tests the reason when mapping census groups");
        Assert.True(provenance > 0, "the reader no longer splits trusted groups by provenance");
        Assert.True(
            reasonTest < provenance,
            "the census mapping splits on provenance BEFORE asking whether the group has an answer at all, "
            + "which files every suppressed bucket as an estimate");
    }

    /// <summary>
    /// R6 and R7: the census and the row read agree which of an index's rows is current, and both reduce the
    /// window to ONE row per index.
    ///
    /// <para>Two distinct failures, one pin. If the tie-breaks differed the census would publish a trusted
    /// count the grid contradicts index by index. And without <c>DISTINCT ON</c> the census would count every
    /// index once per cycle — this collector writes every btree on every run — reporting twice the population
    /// and twice the footprint, which is wrong in the direction that flatters coverage.</para>
    /// </summary>
    [Fact]
    public void TheCensusAndTheRowReadAgreeWhichRowIsCurrent()
    {
        const string TieBreak = "(skipped_reason IS NULL) DESC, collection_time DESC";
        const string Identity = "DISTINCT ON (database_name, schema_name, table_name, index_name)";

        foreach (var (name, sql) in new[]
        {
            ("the row read", DarlingPgIndexBloatReader.PgIndexBloatSql),
            ("the coverage census", DarlingPgIndexBloatReader.CoverageEvidenceSql),
        })
        {
            Assert.Contains(TieBreak, sql);
            Assert.Contains(Identity, sql);
            Assert.False(string.IsNullOrWhiteSpace(name));
        }
    }

    /// <summary>
    /// R8: no evidence is not the same as no indexes, and an ERRORING collector must not answer "nothing to
    /// fix".
    ///
    /// <para>The pair the live store confirms. Measured on an Aurora target at 2026-09-10, this collector
    /// recorded 7 runs in seven days and 3 of them ERRORED — one a 300-second client-side command deadline
    /// that stored nothing at all. So the failing-run case is not hypothetical here, and an unfiltered run
    /// probe would report "it ran" over zero rows and take the innocent arm.</para>
    /// </summary>
    [Fact]
    public void NoEvidenceIsNotTheSameAsNoIndexes()
    {
        var measured = PgIndexBloatCoverage.Classify(true, Nothing, Nothing, [], 0);
        var unmeasured = PgIndexBloatCoverage.Classify(false, Nothing, Nothing, [], 0);

        Assert.Equal(PgIndexBloatCoverageArm.NoCandidates, measured.Arm);
        Assert.Equal(PgIndexBloatCoverageArm.Undetermined, unmeasured.Arm);
        Assert.NotEqual(measured.Cause, unmeasured.Cause);

        /* A measured zero prints as a FIGURE; an unmeasured one prints as a WORD. Rendering both as 0 is how
           "we looked and there is nothing" became indistinguishable from "we never looked". */
        var label = "independently of your row limit): ";

        Assert.Contains(label + "0 holding 0 B", measured.Census, StringComparison.Ordinal);
        Assert.DoesNotContain(PgIndexBloatCoverage.NotMeasured, measured.Census, StringComparison.Ordinal);
        Assert.Contains(
            label + PgIndexBloatCoverage.NotMeasured, unmeasured.Census, StringComparison.Ordinal);

        /* A THIRD token, and it is not decoration. The byte-weighted share is 0 of 0 bytes on the measured
           arm - undefined arithmetic over a real measurement - and the first draft printed NotMeasured
           there, which put the no-evidence word on the one arm that HAS evidence and defeated the
           assertion above. Failing toward "we did not look" is the wrong direction even for a ratio. */
        Assert.Contains(PgIndexBloatCoverage.NotApplicable, measured.Census, StringComparison.Ordinal);
        Assert.DoesNotContain(
            PgIndexBloatCoverage.NotApplicable, unmeasured.Census, StringComparison.Ordinal);
        Assert.NotEqual(PgIndexBloatCoverage.NotApplicable, PgIndexBloatCoverage.NotMeasured);

        /* The Undetermined arm must not smuggle a verdict in, and must name the erroring-collector case as
           one of the things it refuses to distinguish. */
        Assert.Contains("cannot be established", unmeasured.Cause, StringComparison.Ordinal);
        Assert.DoesNotContain("nothing to fix", unmeasured.Cause, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("erroring on every", unmeasured.Cause, StringComparison.Ordinal);

        /* And the RUN PROBE counts only runs that SUCCEEDED - asserted against the SHARED status list, not
           a retyped copy, so the bar for "valid evidence" is the one the freshness reads and the self-alert
           evaluator apply and a status added there reaches this probe. */
        var census = DarlingPgIndexBloatReader.CoverageEvidenceSql;

        Assert.Contains("FROM collection_log", census, StringComparison.Ordinal);
        Assert.Contains("collector_name = 'pg_index_bloat'", census, StringComparison.Ordinal);
        Assert.Contains("FROM pg_index_bloat", census, StringComparison.Ordinal);
        Assert.Contains("AND   status IN (", census, StringComparison.Ordinal);

        foreach (var status in EnumeratedCollectorDriver.FreshnessSuccessStatuses)
        {
            Assert.Contains("'" + status + "'", census, StringComparison.Ordinal);
        }

        /* The probe is a single-row relation the groups hang off, which is what lets a server whose
           collector ran and stored NOTHING still report that it ran. An inner join would drop exactly the
           row separating NoCandidates from Undetermined. */
        Assert.Contains("LEFT JOIN grouped ON true", census, StringComparison.Ordinal);

        /* The two Undetermined situations are deliberately NOT one sentence: "this collector does not run
           here" is the design working on a replica, and "the store read failed" is a monitoring fault. */
        Assert.NotEqual(
            unmeasured.Cause,
            PgIndexBloatCoverage.EvidenceUnreadable(0).Cause);
    }

    /// <summary>
    /// R9: the evidence lookback is a FIXED span ending where the read ends, not the caller's window — and
    /// the label the census prints is that same figure.
    /// </summary>
    [Fact]
    public void TheEvidenceLookbackIsFixedRatherThanTakenFromTheCallersWindow()
    {
        var end = new DateTime(2026, 9, 10, 20, 0, 0, DateTimeKind.Utc);
        var hours = PgIndexBloatCoverage.EvidenceHours;

        Assert.Equal(end.AddHours(-hours), DarlingPgIndexBloatReader.EvidenceStart(end));

        /* Anchored on the END, so an as_of read gets evidence contemporary with the data it explains. */
        var earlier = end.AddDays(-10);
        Assert.Equal(earlier.AddHours(-hours), DarlingPgIndexBloatReader.EvidenceStart(earlier));

        /* MORE than one of the subject collector's cadences, which is the whole derivation. pg_index_bloat
           runs every 1440 minutes, so a lookback of exactly one cadence straddles zero or one run and a
           single lost cycle manufactures Undetermined - measured, 3 of 7 runs errored in a week on one
           live target. Two cadences is the smallest span that survives one lost cycle. */
        Assert.True(
            hours > 24,
            $"the evidence lookback is {hours}h, which is one of pg_index_bloat's own 1440-minute cadences "
            + "or less - a single lost cycle then reads as no evidence");
        Assert.True(
            hours >= 48,
            $"the evidence lookback is {hours}h, shorter than two of this collector's cadences");

        /* AND THE LABEL DESCRIBES THE SPAN THE QUERY USED. They are one figure by construction, and "by
           construction" is what a retyped literal quietly ends - this label is the only thing telling a
           reader that the population figures and the returned row count span different intervals, which is
           what makes EvidenceStale intelligible rather than a contradiction. */
        Assert.Contains(
            hours.ToString(Inv) + "h",
            PgIndexBloatCoverage.EvidenceHoursDescription,
            StringComparison.Ordinal);

        foreach (var (name, verdict, _) in s_cases)
        {
            Assert.Contains(
                PgIndexBloatCoverage.EvidenceHoursDescription,
                verdict.Census,
                StringComparison.Ordinal);
            Assert.False(string.IsNullOrWhiteSpace(name));
        }

        /* AND THE VERDICT READ ACTUALLY APPLIES IT. Everything above tests a pure function; deleting the
           one call that applies it leaves every assertion here green, which is the seam this defect class
           keeps arriving through - a correct mechanism nothing reaches. */
        var verdictRead = CSharpSourceWalker.StripCommentsAndStrings(
            MemberBody(ReaderFile, "GetCoverageVerdictAsync"));

        Assert.Contains("EvidenceStart(endUtc)", verdictRead, StringComparison.Ordinal);

        /* And it does not ACCEPT a window start it would then ignore. The SIGNATURE is the guard, not the
           body: a parameter passed and dropped is worse than an absent one, because the caller believes its
           window was honoured and neither the compiler nor the answer says otherwise. Scoped to the
           declaration so the ROW read's own startUtc - which it does use - is out of scope by
           construction. */
        var verdictSignature = MemberSignature(ReaderFile, "GetCoverageVerdictAsync");

        Assert.DoesNotContain("startUtc", verdictSignature, StringComparison.Ordinal);
        Assert.Contains("DateTime endUtc", verdictSignature, StringComparison.Ordinal);

        /* The raw-groups reader is the opposite case and stays that way: it takes both ends BECAUSE it uses
           both, and it is what a caller wanting its own span reaches for. Asserting it keeps the narrowing
           above from being read as "windows are not a thing here". */
        var evidenceSignature = MemberSignature(ReaderFile, "GetCoverageEvidenceAsync");

        Assert.Contains("DateTime startUtc", evidenceSignature, StringComparison.Ordinal);
        Assert.Contains("DateTime endUtc", evidenceSignature, StringComparison.Ordinal);
    }

    /// <summary>
    /// R10: the reason markers still match the prose the collector actually writes.
    ///
    /// <para>Read from the SHIPPED query rather than from a retyped copy. A marker that has drifted sends
    /// every row of its bucket to <see cref="PgIndexBloatSuppression.Unrecognized"/> — which fails toward
    /// "we cannot attribute this" rather than toward a wrong remedy, deliberately, and is still a whole
    /// bucket losing its name with nothing that complains.</para>
    /// </summary>
    [Fact]
    public void EveryReasonMarkerStillAppearsInTheShippedCollectorQuery()
    {
        var collectorSql = PgIndexBloatCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 1,
            ServerName = "server",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
        }).Text;

        foreach (var (marker, reason) in PgIndexBloatCoverage.Markers)
        {
            Assert.Contains(marker, collectorSql, StringComparison.OrdinalIgnoreCase);
            Assert.NotEqual(PgIndexBloatSuppression.Unrecognized, reason);
        }

        /* Every named bucket HAS a marker, derived from the enum rather than from a list somebody remembered
           to extend - a seventh reason added to the collector with no marker here would arrive as
           Unrecognized on live data and pass every assertion above. */
        Assert.Equal(
            Enum.GetValues<PgIndexBloatSuppression>()
                .Where(r => r != PgIndexBloatSuppression.Unrecognized)
                .OrderBy(r => r)
                .ToArray(),
            PgIndexBloatCoverage.Markers.Select(m => m.Reason).Distinct().OrderBy(r => r).ToArray());

        /* Sorted longest-first BY CONSTRUCTION, so a marker that is also the start of another cannot decide
           the match by declaration accident. */
        var lengths = PgIndexBloatCoverage.Markers.Select(m => m.Marker.Length).ToArray();

        Assert.Equal(lengths.OrderByDescending(l => l).ToArray(), lengths);

        /* Each marker actually selects its own bucket off the FULL stored prose, not merely off itself. */
        Assert.Equal(
            PgIndexBloatSuppression.ColumnWidthsNotVisible,
            PgIndexBloatCoverage.Bucket(
                "column widths are not visible for every key: pg_stats filters on has_column_privilege, so "
                + "a monitoring role without SELECT sees nothing"));
        Assert.Equal(
            PgIndexBloatSuppression.Partial,
            PgIndexBloatCoverage.Bucket("partial index: the model scales the parent reltuples"));

        /* And unknown prose does NOT take a named bucket - it fails toward "we do not know which", which is
           the direction that keeps its bytes in the suppressed total rather than attaching a wrong remedy
           to them. */
        Assert.Equal(
            PgIndexBloatSuppression.Unrecognized,
            PgIndexBloatCoverage.Bucket("a reason some later collector writes"));
        Assert.Equal(PgIndexBloatSuppression.Unrecognized, PgIndexBloatCoverage.Bucket(null));
        Assert.Equal(PgIndexBloatSuppression.Unrecognized, PgIndexBloatCoverage.Bucket("   "));

        /* Unrecognized is the enum's DEFAULT, so a bucket nobody set lands on "we do not know which"
           instead of on a named cause with a remedy attached. */
        Assert.Equal(PgIndexBloatSuppression.Unrecognized, default(PgIndexBloatSuppression));

        /* Every reason has a remedy and a label, and no two share either - the remedy is what an operator
           acts on, so two buckets rendering the same one is a bucket that cannot be acted on correctly. */
        var reasons = Enum.GetValues<PgIndexBloatSuppression>();

        Assert.Equal(
            reasons.Length,
            reasons.Select(PgIndexBloatCoverage.Remedy).Distinct(StringComparer.Ordinal).Count());
        Assert.Equal(
            reasons.Length,
            reasons.Select(PgIndexBloatCoverage.Label).Distinct(StringComparer.Ordinal).Count());

        /* The remediable and the structural say which they are, because that decides whether an operator
           has anything to do at all. */
        Assert.Contains(
            "GRANT pg_read_all_data",
            PgIndexBloatCoverage.Remedy(PgIndexBloatSuppression.ColumnWidthsNotVisible),
            StringComparison.Ordinal);
        Assert.Contains(
            "ANALYZE",
            PgIndexBloatCoverage.Remedy(PgIndexBloatSuppression.ParentNeverAnalyzed),
            StringComparison.Ordinal);

        foreach (var structural in new[]
        {
            PgIndexBloatSuppression.Partial,
            PgIndexBloatSuppression.NegativeBloat,
            PgIndexBloatSuppression.NameTypedKey,
        })
        {
            var remedy = PgIndexBloatCoverage.Remedy(structural);

            Assert.Contains("STRUCTURAL", remedy, StringComparison.Ordinal);
            Assert.DoesNotContain("GRANT", remedy, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// R3: different arm, different verdict — over DIFFERING arms rather than over every case, because two
    /// inputs reaching the same arm are REQUIRED to render the same cause.
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

        /* Two inputs differing only in their FIGURES reach the same arm and must render the same cause -
           otherwise the text is a function of the numbers rather than of the verdict. */
        Assert.Equal(
            PgIndexBloatCoverage.Classify(true, new PgIndexBloatTally(1, Gib), Nothing, FleetBuckets, 25).Cause,
            PgIndexBloatCoverage.Classify(true, FleetTrusted, Nothing, FleetBuckets, 1_000).Cause);
    }

    /// <summary>
    /// R3's falsifier. Without this, <see cref="NoTwoDifferentArmsRenderTheSameCause"/> is satisfied by a
    /// classifier that had stopped selecting anything and merely echoed its inputs — the arms can never hold
    /// identical figures, so their composed messages differ whatever the verdict says.
    ///
    /// <para>Every arm, not one: a probe set that reached only some of them would make "no arm echoes its
    /// inputs" a claim about however many this list happens to visit.</para>
    /// </summary>
    [Fact]
    public void TheCauseCarriesNoneOfTheInputFigures()
    {
        var loud = new PgIndexBloatTally(424_242, 777 * Gib);
        var quieter = new PgIndexBloatTally(31_337, 555 * Mib);
        var buckets = new[] { Bucket(PgIndexBloatSuppression.Partial, 90_210, 333 * Gib) };

        var probes = new[]
        {
            PgIndexBloatCoverage.Classify(false, loud, quieter, buckets, 8_675_309),
            PgIndexBloatCoverage.Classify(true, Nothing, Nothing, [], 0),
            PgIndexBloatCoverage.Classify(true, Nothing, Nothing, buckets, 8_675_309),
            PgIndexBloatCoverage.Classify(true, loud, quieter, buckets, 8_675_309),
            PgIndexBloatCoverage.Classify(true, loud, quieter, [], 8_675_309),
            PgIndexBloatCoverage.Classify(true, Nothing, Nothing, [], 8_675_309),
            PgIndexBloatCoverage.EvidenceUnreadable(8_675_309),
        };

        /* And the probe set really does visit every arm. */
        Assert.Equal(
            Enum.GetValues<PgIndexBloatCoverageArm>().OrderBy(a => a).ToArray(),
            probes.Select(p => p.Arm).Distinct().OrderBy(a => a).ToArray());

        var figures = new[]
        {
            "424242", "424,242", "31337", "31,337", "90210", "90,210", "8675309", "8,675,309",
            "777.0 GB", "555.0 MB", "333.0 GB",
        };

        foreach (var probe in probes)
        {
            foreach (var figure in figures)
            {
                Assert.DoesNotContain(figure, probe.Cause, StringComparison.Ordinal);
            }
        }

        /* And the figures are not merely absent from the cause - they are PRESENT in the census, or every
           check above is satisfied by a class that had stopped reporting them at all. */
        var populated = probes[3].Census;

        foreach (var figure in new[]
            { "424,242", "31,337", "90,210", "8,675,309", "777.0 GB", "555.0 MB", "333.0 GB" })
        {
            Assert.Contains(figure, populated, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// R2 and R3: both surfaces print the SHARED verdict, and neither builds one of its own.
    ///
    /// <para>The MCP tool and the WPF panel answer the same question for the same operator, so a fix applied
    /// to one of them is this defect class's usual shape. Scanning for the shared call is what makes "they
    /// cannot disagree" a property rather than a hope.</para>
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
            Assert.DoesNotContain("new PgIndexBloatCoverageVerdict", code, StringComparison.Ordinal);

            /* Nor may it re-rank the buckets. The classifier orders them by bytes; a surface applying its
               own OrderBy is how one operator gets two "largest cause" answers from one store. */
            Assert.DoesNotContain("Suppressed.OrderBy", code, StringComparison.Ordinal);
            Assert.DoesNotContain("Suppressed.OrderByDescending", code, StringComparison.Ordinal);
        }

        /* The viewer reaches the classifier through its data service, so the CHAIN is what the property is
           about - pinning the panel's call alone would leave a passthrough free to author its own answer. */
        var passthrough = CSharpSourceWalker.StripCommentsAndStrings(MemberBody(
            "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.Postgres.cs",
            "GetPgIndexBloatCoverageAsync"));

        Assert.Contains(
            "DarlingPgIndexBloatReader.GetCoverageVerdictAsync", passthrough, StringComparison.Ordinal);
    }

    /// <summary>
    /// R2 at the surface. The census has to ride the POPULATED branch as well, or a truncated page is
    /// explained only in the one case where nobody needs the explanation — and the truncated page is the
    /// whole defect. Asserted by counting the prints: one branch printing it twice would satisfy a presence
    /// check.
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
                $"{member} prints the coverage census {prints} time(s); it has to reach the empty result AND "
                + "the populated one, or a page of answerless rows is only ever explained when it is empty");
        }
    }

    /// <summary>
    /// The three miss answers are asked in <c>CollectorRuntimePrecondition</c>'s documented order —
    /// capability, then precondition, then this read's own miss — and the coverage census is not queried
    /// ahead of the branch that might not need it.
    ///
    /// <para>The cost is the visible half: a server that cannot have this surface at all, or whose collector
    /// recorded a denial, would pay for a census the chain then discards. The half worth guarding is the
    /// other one: computing the LAST of three ranked answers FIRST is how somebody later reorders the chain
    /// and does not notice they have changed which one wins. A source-order assertion is the only thing that
    /// notices, because every ordering compiles and every ordering returns a plausible answer.</para>
    /// </summary>
    [Fact]
    public void TheMissAnswersAreAskedInTheirDocumentedPrecedenceOrder()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(MemberBody(
            "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgIndexTools.cs",
            "GetPgIndexBloat"));

        var branch = code.IndexOf("rows.Count == 0", StringComparison.Ordinal);
        var capability = code.IndexOf("NotCollectedStatusAsync", StringComparison.Ordinal);
        var precondition = code.IndexOf("DarlingRuntimePrecondition.StatusAsync", StringComparison.Ordinal);
        var coverage = code.IndexOf("GetCoverageVerdictAsync", StringComparison.Ordinal);

        Assert.True(branch > 0, "the empty-result branch is gone");
        Assert.True(capability > 0, "the capability answer is gone");
        Assert.True(precondition > 0, "the precondition answer is gone");
        Assert.True(coverage > 0, "the coverage answer is gone");

        Assert.True(
            branch < coverage,
            "the coverage census is queried BEFORE the branch that decides whether anything needs it");
        Assert.True(
            capability < precondition,
            "a precondition is answered ahead of a permanent engine gap, which #2511 closed");
        Assert.True(
            precondition < coverage,
            "the coverage verdict is computed ahead of the precondition that outranks it");
    }

    /// <summary>
    /// The invariant, swept over the whole input space: <b>the verdict never contradicts the row set printed
    /// beside it.</b>
    ///
    /// <para>This enumerates input REGIONS where the arm cases above enumerate ARMS, and that is the
    /// difference between finding a wrong assertion and finding a missing one. The population figures and
    /// the returned row count are measured over different spans — a fixed
    /// <see cref="PgIndexBloatCoverage.EvidenceHours"/> lookback against the caller's own window — so zero
    /// candidates beside a non-empty result is reachable, and it is the combination that took the innocent
    /// arm one collector over.</para>
    ///
    /// <para>Two directions, because the contradiction has two: a non-empty result must never be told there
    /// is nothing here, and an empty one must never be told coverage is whole or partial — partial coverage
    /// of nothing is not something an operator can act on.</para>
    /// </summary>
    [Fact]
    public void NoVerdictContradictsTheRowSetPrintedBesideIt()
    {
        var seen = new HashSet<PgIndexBloatCoverageArm>();
        var regions = 0;

        foreach (var ran in new[] { false, true })
        foreach (var trusted in new[] { 0L, 1L, 917L })
        foreach (var suppressed in new[] { 0L, 1L, 6_680L })
        foreach (var returned in new[] { 0, 1, 25, 1_000 })
        {
            regions++;

            var verdict = PgIndexBloatCoverage.Classify(
                ran,
                new PgIndexBloatTally(trusted, trusted * Gib),
                Nothing,
                suppressed == 0
                    ? []
                    : [Bucket(PgIndexBloatSuppression.ColumnWidthsNotVisible, suppressed, suppressed * Gib)],
                returned);

            seen.Add(verdict.Arm);

            var inputs = $"ran={ran} trusted={trusted} suppressed={suppressed} returned={returned} "
                + $"-> {verdict.Arm}";

            Assert.False(string.IsNullOrWhiteSpace(verdict.Cause), "unclassified region: " + inputs);
            Assert.False(string.IsNullOrWhiteSpace(verdict.Census), "censusless region: " + inputs);

            /* The census figures always agree with the verdict's own tallies, whatever the arm - a census
               computed off different numbers than the arm was decided from is the contradiction this whole
               class is about, arriving inside the fix. */
            Assert.Equal(
                verdict.Trusted.IndexCount + verdict.Suppressed.Sum(b => b.IndexCount),
                verdict.Candidates.IndexCount);

            if (returned > 0)
            {
                Assert.NotEqual(PgIndexBloatCoverageArm.NoCandidates, verdict.Arm);
                Assert.DoesNotContain("Nothing to fix", verdict.Cause, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("NO CANDIDATES", verdict.Cause, StringComparison.Ordinal);
            }

            if (trusted > 0 && ran)
            {
                /* Coverage exists, so no arm may claim there is none. */
                Assert.NotEqual(PgIndexBloatCoverageArm.NothingTrusted, verdict.Arm);
                Assert.DoesNotContain("NO COVERAGE AT ALL", verdict.Cause, StringComparison.Ordinal);
            }

            if (suppressed > 0 && ran)
            {
                /* Something is withheld, so no arm may claim the ranking describes the whole server. */
                Assert.NotEqual(PgIndexBloatCoverageArm.FullyTrusted, verdict.Arm);
                Assert.DoesNotContain("FULL COVERAGE", verdict.Cause, StringComparison.Ordinal);
            }

            if (!ran)
            {
                /* An unmeasured region publishes no population figure at all - not a zero, and not the
                   figures it was handed. Failing toward "we do not know" is the direction that costs least
                   when it is wrong. */
                Assert.Equal(PgIndexBloatCoverageArm.Undetermined, verdict.Arm);
                Assert.Equal(default, verdict.Candidates);
                Assert.Empty(verdict.Suppressed);
                Assert.Contains(
                    PgIndexBloatCoverage.NotMeasured, verdict.Census, StringComparison.Ordinal);
            }
        }

        /* The sweep is only worth its assertions if it reaches everything. Both halves: enough regions to be
           a sweep, and every arm visited - an arm the sweep cannot reach is an arm this invariant says
           nothing about. */
        Assert.True(regions >= 60, $"the sweep covered only {regions} region(s)");
        Assert.Equal(
            Enum.GetValues<PgIndexBloatCoverageArm>().OrderBy(a => a).ToArray(),
            seen.OrderBy(a => a).ToArray());
    }

    /// <summary>
    /// How the census renders one count-and-bytes pair, built from the figures rather than typed out. A
    /// literal expectation for a DERIVED figure is a second calculation, and the second one is the one that
    /// is wrong.
    /// </summary>
    private static string Expect(long count, long bytes) =>
        count.ToString("N0", Inv) + " holding "
        + (bytes / (double)Gib).ToString("N1", Inv) + " GB";

    private static PgIndexBloatSuppressionBucket Bucket(
        PgIndexBloatSuppression reason, long count, long bytes) =>
        new(reason, count, bytes, "stored prose for " + reason);

    private static PgIndexBloatCoverageVerdict Case(PgIndexBloatCoverageArm arm) =>
        s_cases.First(c => c.Verdict.Arm == arm).Verdict;

    /// <summary>
    /// The two surfaces that report this collector's coverage to a person, the member on each that does it,
    /// and the accessor that member has to obtain the verdict FROM.
    ///
    /// <para>The accessor differs per surface and that is not incidental: the MCP tool calls the store reader
    /// directly while the panel goes through the viewer's data service, and asserting one name for both would
    /// have to be the looser of the two to pass.</para>
    /// </summary>
    private static IEnumerable<(string File, string Member, string Accessor)> Surfaces =>
    [
        ("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgIndexTools.cs",
            "GetPgIndexBloat", "DarlingPgIndexBloatReader.GetCoverageVerdictAsync"),
        ("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.Postgres.cs",
            "LoadPgIndexBloatAsync", "_dataService.GetPgIndexBloatCoverageAsync"),
    ];

    /// <summary>
    /// One member's brace-balanced body, verbatim. Callers narrow it themselves — the call-presence scans
    /// strip comments and literals — because a single pre-stripped form would answer the wrong question
    /// against the wrong half of the text.
    /// </summary>
    private static string MemberBody(string relativePath, string member)
    {
        var source = ReadSource(relativePath);

        /* The signature is located on a comment-and-string-stripped copy so a mention of the member name
           inside a doc comment cannot be mistaken for its declaration; the OFFSETS are then used against
           the original text, which the walker guarantees are the same because it replaces rather than
           removes.

           The DECLARATION, not the first occurrence: the viewer CALLS LoadPgIndexBloatAsync from the
           storage-tab loader above declaring it, so a first-match scan lands on the call site,
           brace-balances the WRONG method and reports whatever that one happens to contain. Every
           occurrence is classified and exactly one must be a declaration, so a rename or an added overload
           fails loudly rather than silently re-aiming. */
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(source);
        var declarations = Declarations(stripped, member).ToList();

        var at = Assert.Single(declarations);
        var open = stripped.IndexOf('{', at);
        var semicolon = stripped.IndexOf(';', at);

        Assert.True(open > at || semicolon > at, $"{member} in {relativePath} has no body of either shape");

        /* Expression-bodied members are in scope rather than an exception to skip: the viewer's data-service
           passthrough is one, and it is the middle link of the chain this asserts. Whichever terminator
           comes first decides the shape - a `;` before any `{` is `=> expression;`, and reaching for the
           brace regardless would balance the NEXT member's block. */
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
    /// carries <c>await</c>, a receiver, or nothing at all.
    /// </summary>
    private static IEnumerable<int> Declarations(string stripped, string member)
    {
        for (var at = stripped.IndexOf(member + "(", StringComparison.Ordinal);
             at >= 0;
             at = stripped.IndexOf(member + "(", at + 1, StringComparison.Ordinal))
        {
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

    /// <summary>
    /// One member's SIGNATURE — the declaration up to the brace or arrow beginning its body, comments and
    /// literals blanked. Separate from <see cref="MemberBody"/> because a claim about what a method ACCEPTS
    /// cannot be made against its body: the body is where an ignored parameter is conspicuously absent.
    /// </summary>
    private static string MemberSignature(string relativePath, string member)
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(ReadSource(relativePath));
        var declarations = Declarations(stripped, member).ToList();

        var at = Assert.Single(declarations);
        var open = stripped.IndexOf('{', at);
        var arrow = stripped.IndexOf("=>", at, StringComparison.Ordinal);
        var stops = new[] { open, arrow }.Where(i => i > at).ToArray();

        Assert.NotEmpty(stops);

        return stripped[at..stops.Min()];
    }

    private static string ReadSource(string relative)
    {
        var path = Path.Combine(RepoRoot(), relative);

        Assert.True(File.Exists(path), $"#3278 scan target not found: {path}");

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
