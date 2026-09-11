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

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Why one <c>pg_index_bloat</c> row carries a reason instead of an answer, as a SHORT STABLE KEY (#3278).
///
/// <para><b>A key rather than the prose, and that is the whole reason this enum exists.</b> The collector
/// stores its reason as a full explanatory paragraph repeated on every row — measured on the fleet, 2,954
/// copies of the same 250-character sentence in the largest bucket alone. A summary that grouped on that
/// text would carry a quarter of a kilobyte per bucket to say which bucket it is, and a consumer wanting
/// to branch on the cause would be matching prose. So the bucket is keyed here and the prose travels
/// once.</para>
/// </summary>
public enum PgIndexBloatSuppression
{
    /// <summary>
    /// The reason text is not one this build recognises. Reachable and deliberately not an error: the store
    /// holds 90 days of rows, so a reason written by an older or newer collector arrives here rather than
    /// being dropped — an unrecognised bucket still counts toward the suppressed totals, because a bucket
    /// whose bytes vanished from the census would understate the gap it exists to measure.
    ///
    /// <para>First rather than last so <c>default</c> lands on "we do not know which" instead of on a named
    /// cause with a remedy attached.</para>
    /// </summary>
    Unrecognized,

    /// <summary>
    /// The index carries a predicate, so the model's parent <c>reltuples</c> describes a superset of the
    /// rows the index actually holds. Structural: no grant and no <c>ANALYZE</c> reaches it. Second largest
    /// bucket BY BYTES on the fleet at 1,385 GB, and fourth of five by rows.
    /// </summary>
    Partial,

    /// <summary>
    /// The parent table has never been analyzed (<c>reltuples = -1</c>), so there is no row count to model
    /// from. Remediable with one <c>ANALYZE</c> — and the bucket the byte weighting exists to demote: second
    /// largest of five BY ROWS on the fleet and LAST by footprint, at 22 MB of 5,970 GB.
    /// </summary>
    ParentNeverAnalyzed,

    /// <summary>
    /// A key column is <c>name</c>-typed, whose <c>pg_stats</c> width is the padded 64 bytes rather than the
    /// stored text. Structural: the statistic exists and is the wrong quantity, so no grant improves it.
    /// </summary>
    NameTypedKey,

    /// <summary>
    /// <c>pg_stats</c> did not yield a width for every key column. Remediable, and the largest bucket by
    /// footprint on the fleet at 4,144 GB — <c>pg_stats</c> filters on <c>has_column_privilege</c>, so the
    /// commonest cause is a monitoring login without SELECT rather than anything about the index.
    /// </summary>
    ColumnWidthsNotVisible,

    /// <summary>
    /// The index occupies no pages yet, so there is nothing for it to be holding. Nothing to fix and nothing
    /// to grant — the one suppression bucket that is a measurement rather than a gap.
    /// </summary>
    NoPagesYet,

    /// <summary>
    /// The model predicts more pages than the index occupies. Structural on PostgreSQL 13 and above, which
    /// stores duplicate keys once in a posting list: real storage is denser than per-tuple arithmetic can
    /// predict, so a CORRECT model still over-predicts and the shortfall is not an answer.
    /// </summary>
    NegativeBloat,
}

/// <summary>
/// One suppression bucket: which reason, how many indexes, how many BYTES of index those hold, and the
/// collector's own prose for it carried once rather than per row.
/// </summary>
/// <param name="Reason">The stable key. <see cref="PgIndexBloatSuppression.Unrecognized"/> when the stored
/// prose matches no marker this build knows.</param>
/// <param name="IndexCount">Indexes in this bucket.</param>
/// <param name="IndexBytes">Their combined <c>index_bytes</c>. Not optional and not decoration: on the live
/// fleet the row ranking and the byte ranking of these buckets DISAGREE, so a census carrying only counts
/// points an operator at the bucket that cannot matter.</param>
/// <param name="Detail">The collector's stored explanation, once. Null when nothing supplied one.</param>
public readonly record struct PgIndexBloatSuppressionBucket(
    PgIndexBloatSuppression Reason,
    long IndexCount,
    long IndexBytes,
    string? Detail);

/// <summary>
/// A count and the bytes it accounts for, as one value — so a caller cannot pass or print the one without
/// the other, which is the defect <see cref="PgIndexBloatCoverage"/> exists to close.
/// </summary>
public readonly record struct PgIndexBloatTally(long IndexCount, long IndexBytes)
{
    /// <summary>The two figures added, for composing a total out of its parts.</summary>
    public static PgIndexBloatTally operator +(PgIndexBloatTally left, PgIndexBloatTally right) =>
        new(left.IndexCount + right.IndexCount, left.IndexBytes + right.IndexBytes);
}

/// <summary>
/// What a <c>get_pg_index_bloat</c> read's coverage IS, over the server rather than over the page returned
/// (#3278). Named arms rather than a ratio, because the population figures and the rows are measured over
/// different spans and a ratio drawn across them is not a coverage claim.
/// </summary>
public enum PgIndexBloatCoverageArm
{
    /// <summary>
    /// No usable evidence for this server in the evidence window, so no arm can be selected. The arm that
    /// keeps the others honest: without it a collector that errored every cycle in the window presents
    /// as a server with no indexes, which is a confident all-clear built on no measurement.
    /// </summary>
    Undetermined,

    /// <summary>
    /// The collector ran and the window holds no candidate index at all. The census is correct and empty,
    /// and there is nothing for an operator to do.
    /// </summary>
    NoCandidates,

    /// <summary>
    /// Candidates exist and NOT ONE of them has a trusted answer. This is the fleet's measured state on a
    /// target whose monitoring login cannot read <c>pg_stats</c>, and it is the arm a truncated page of
    /// all-skipped rows has been misread as ever since the estimate arm shipped.
    /// </summary>
    NothingTrusted,

    /// <summary>
    /// Some candidates have an answer and some do not. The ranking is over a subset of the server's index
    /// footprint, which is the same defect as a total absence one degree weaker — the grid looks complete
    /// and is not.
    /// </summary>
    PartialCoverage,

    /// <summary>Every candidate index has a trusted answer. The ranking describes the whole server.</summary>
    FullyTrusted,

    /// <summary>
    /// The read returned rows and the evidence window holds no candidate index — so the two describe
    /// different populations and neither a coverage ratio nor an emptiness verdict can be claimed.
    ///
    /// <para>Reachable because the spans differ: the evidence is a fixed
    /// <see cref="PgIndexBloatCoverage.EvidenceHoursDescription"/> lookback while the rows are over whatever
    /// window the caller asked for, which defaults to seven days. An index dropped since, a database
    /// removed from the enumeration, or a collector that has errored for longer than the evidence window
    /// all leave rows behind with no candidate to match them. Without this arm that combination took
    /// <see cref="NoCandidates"/> and printed "nothing to do" beside a non-empty result.</para>
    /// </summary>
    EvidenceStale,
}

/// <summary>
/// One <see cref="PgIndexBloatCoverageArm"/>, the population figures it was decided from, the verdict
/// sentence, and the suppression breakdown — as one value, so no surface can print the arm without its
/// figures or the figures without the arm.
///
/// <para><b><see cref="Census"/> and <see cref="Cause"/> are separate on purpose.</b> The census is the
/// figures, in one shape for every arm; the cause is the verdict and carries NO figures at all. A guard
/// that the arms are distinguishable has to compare the CAUSE — comparing the composed message would pass
/// on the figures differing, which they always do, and would then report discrimination this class had
/// stopped providing.</para>
/// </summary>
/// <param name="Suppressed">The buckets, ranked BY BYTES descending. Carried on the verdict rather than
/// left at the call site so the MCP payload and the WPF note rank them identically.</param>
public readonly record struct PgIndexBloatCoverageVerdict(
    PgIndexBloatCoverageArm Arm,
    string Census,
    string Cause,
    PgIndexBloatTally Candidates,
    PgIndexBloatTally Estimated,
    PgIndexBloatTally ExactlyMeasured,
    IReadOnlyList<PgIndexBloatSuppressionBucket> Suppressed)
{
    /// <summary>Estimated plus exactly measured, derived rather than passed — see the classifier.</summary>
    public PgIndexBloatTally Trusted => Estimated + ExactlyMeasured;

    /// <summary>The census then the cause, for the surfaces that print one string.</summary>
    public string Message => Census + " " + Cause;
}

/// <summary>
/// What share of a server's index footprint <c>get_pg_index_bloat</c> actually has an answer for, and the
/// one sentence that says so (#3278).
///
/// <para><b>The defect this closes.</b> The reader sorts rows carrying a reason FIRST, by design — an index
/// too large or too odd to model is the likeliest big win, so filtering it out would hide the biggest
/// candidate. The consequence is that any <c>LIMIT</c> smaller than the suppressed population returns 100%
/// suppressed rows, structurally rather than probabilistically, and raising the limit does not fix it while
/// the suppressed population is still larger. Four independent sessions misread that surface in one night
/// on 2026-09-10, three through the limit and a fourth through the store directly — and every one of them
/// had read the "unmeasured first" design comment first. Three careful readers reaching the same wrong
/// conclusion is a property of the surface.</para>
///
/// <para><b>Why bytes are mandatory rather than a refinement.</b> Measured on the fleet at 2026-09-10, 50
/// targets and 7,597 rows over 5,970 GB of index: <see cref="PgIndexBloatSuppression.ParentNeverAnalyzed"/>
/// is the SECOND largest bucket by rows and the LAST by footprint, at 22 MB — 0.0004% of it. Meanwhile
/// <see cref="PgIndexBloatSuppression.Partial"/> is fourth of five by rows and second by bytes at 1,385 GB.
/// Both orderings look defensible and only one is useful, so a census reporting counts alone would point an
/// operator squarely at the bucket that cannot matter.</para>
///
/// <para><b><c>skipped_reason IS NULL</c> is the only trust predicate, and nothing in the column names says
/// so.</b> <c>est_tuple_bytes</c> is populated on 100% of suppressed rows in every bucket — 2954/2954,
/// 2225/2225, 803/803, 698/698 — while <c>est_bloat_pct</c> is populated on 0% of them. The intermediate is
/// universally present and both conclusions are universally NULL, so a reader who filters on the
/// intermediate gets every row and believes they have complete coverage. That is incident four, and it cost
/// a reported "461 GB now covered" against a real 214 GB of 5,954.</para>
///
/// <para><b>Pure policy, no clock and no I/O.</b> The caller reads the tallies and passes them in, the same
/// discipline <c>PgColumnStatsCoverage</c> follows — which is what lets the arm selection be asserted
/// without a store, and what lets one classifier serve the MCP tool and the WPF panel so the two cannot
/// disagree about what a page of skipped rows means.</para>
/// </summary>
public static class PgIndexBloatCoverage
{
    /// <summary>
    /// What the census prints where a figure should be when there is no measurement to put there. A word,
    /// never a zero: "0 candidate indexes" is a finding and "we did not look" is not, and rendering those
    /// two identically is the whole of this defect class.
    /// </summary>
    public const string NotMeasured = "not measured";

    /// <summary>
    /// What the census prints where a RATIO should be when the population was measured and has no footprint
    /// to take a share of. Distinct from <see cref="NotMeasured"/>, and the distinction is the same one this
    /// class exists for one level down: 0 of 0 bytes is undefined arithmetic over a real measurement, and
    /// rendering it as "we did not look" would put the no-evidence word on the one arm that has evidence.
    /// </summary>
    public const string NotApplicable = "not applicable";

    /// <summary>
    /// How far back the population figures are measured, and the single definition of it — the store reader
    /// applies this rather than holding a copy, because the figure appears in operator-facing text and a
    /// second definition is how the label stops describing the query.
    ///
    /// <para><b>Two of the subject collector's own cadences, not one.</b> <c>pg_index_bloat</c> runs every
    /// 1440 minutes, so a lookback of ONE cadence straddles zero or one run and any jitter or single lost
    /// cycle manufactures <see cref="PgIndexBloatCoverageArm.Undetermined"/> on a server holding a week of
    /// evidence. That is not hypothetical for this collector: measured on a live Aurora target on
    /// 2026-09-10 it recorded 7 runs in seven days and 3 of them ERRORED — a 42.9% failure rate, one of
    /// them a 300-second client-side command deadline that stored nothing at all. A single-cadence window
    /// landing on that cycle would have answered "coverage cannot be established" beside six days of
    /// perfectly good census rows. Two cadences is the smallest span that survives one lost cycle, which is
    /// the property <c>PgColumnStatsCoverage.EvidenceHours</c> gets for free by spanning twenty-four of its
    /// evidence collector's hourly runs.</para>
    ///
    /// <para>It is deliberately NOT the caller's window — see the store reader's <c>EvidenceStart</c> for
    /// why that is wrong in both directions, and <see cref="PgIndexBloatCoverageArm.EvidenceStale"/> for
    /// what happens when the two spans disagree.</para>
    /// </summary>
    public const int EvidenceHours = 48;

    /// <summary>
    /// How the census labels the span its population figures are measured over, derived from
    /// <see cref="EvidenceHours"/> rather than retyped. Spelled out beside the figures because the returned
    /// row count is measured over a DIFFERENT span — the caller's own window, and subject to the caller's
    /// own limit — and a reader drawing a ratio across them has to know that before doing it.
    /// </summary>
    public static readonly string EvidenceHoursDescription =
        "measured over the last " + EvidenceHours.ToString(CultureInfo.InvariantCulture) + "h";

    /// <summary>
    /// The <c>GRANT</c> remedy, one copy. It names the PostgreSQL 14+ role because that is the grant
    /// measured to restore agreement with a superuser's numbers (#2542), and the explicit GRANT beside it
    /// because 13 has no such role.
    /// </summary>
    public const string PrivilegeRemedy =
        "Remedy: grant the monitoring login read access - GRANT pg_read_all_data TO <login> on PostgreSQL 14 "
        + "and above, or GRANT SELECT on the parent tables on 13. Row-level security empties pg_stats by the "
        + "same filter, so check for policies on these tables if the grant is already in place.";

    /// <summary>
    /// The prose prefix the collector writes for each bucket, and the key it maps to. Prefixes rather than
    /// substring probes, and read from the collector's own literals — a marker that has drifted from the
    /// shipped SQL sends every row of that bucket to
    /// <see cref="PgIndexBloatSuppression.Unrecognized"/>, which is why a pin asserts each one still appears
    /// in the query the collector actually issues.
    ///
    /// <para>Sorted longest-marker-first BY CONSTRUCTION rather than by how they are typed below, so a
    /// marker that is also the start of another cannot decide the match by declaration accident. None
    /// currently overlap; sorting is what keeps a later addition from making the declaration order
    /// load-bearing, and what keeps the claim in this sentence from depending on somebody counting
    /// characters correctly.</para>
    /// </summary>
    public static readonly IReadOnlyList<(string Marker, PgIndexBloatSuppression Reason)> Markers =
    [
        .. new (string Marker, PgIndexBloatSuppression Reason)[]
        {
            ("partial index:", PgIndexBloatSuppression.Partial),
            ("the parent table has never been analyzed", PgIndexBloatSuppression.ParentNeverAnalyzed),
            ("a key column is name-typed", PgIndexBloatSuppression.NameTypedKey),
            ("column widths are not visible for every key", PgIndexBloatSuppression.ColumnWidthsNotVisible),
            ("the index occupies no pages yet", PgIndexBloatSuppression.NoPagesYet),
            ("the model predicts more pages than the index occupies", PgIndexBloatSuppression.NegativeBloat),
        }.OrderByDescending(entry => entry.Marker.Length),
    ];

    /// <summary>
    /// Which bucket a stored <c>skipped_reason</c> belongs to. Unknown prose answers
    /// <see cref="PgIndexBloatSuppression.Unrecognized"/> rather than throwing or guessing: the store holds
    /// 90 days of rows and the reason text is not a contract, so failing toward "we do not know which
    /// bucket" keeps the bytes in the census where a dropped row would take them out of it.
    /// </summary>
    public static PgIndexBloatSuppression Bucket(string? skippedReason)
    {
        if (string.IsNullOrWhiteSpace(skippedReason))
        {
            return PgIndexBloatSuppression.Unrecognized;
        }

        var reason = skippedReason.TrimStart();

        foreach (var (marker, bucket) in Markers)
        {
            if (reason.StartsWith(marker, StringComparison.OrdinalIgnoreCase))
            {
                return bucket;
            }
        }

        return PgIndexBloatSuppression.Unrecognized;
    }

    /// <summary>
    /// What to DO about one bucket, and whether anything can be. Separate from the prose the collector
    /// stores, which explains the modelling rather than naming the action.
    /// </summary>
    public static string Remedy(PgIndexBloatSuppression reason) =>
        reason switch
        {
            PgIndexBloatSuppression.ColumnWidthsNotVisible =>
                "pg_stats yielded no width for at least one key column, and on a managed fleet the usual "
                + "cause is the monitoring login rather than the index: pg_stats filters on "
                + "has_column_privilege and pg_monitor does not confer SELECT. " + PrivilegeRemedy
                + " A never-analyzed parent empties the same view, so ANALYZE the parents that stay dark "
                + "after the grant lands.",
            PgIndexBloatSuppression.ParentNeverAnalyzed =>
                "Remedy: ANALYZE the parent tables. reltuples is -1 until the first analyze, so there is no "
                + "row count to model from and one pass makes these estimable.",
            PgIndexBloatSuppression.Partial =>
                "STRUCTURAL, not remediable: the model scales the parent's row count and the index holds "
                + "only the rows matching its predicate. No grant and no ANALYZE reaches these - "
                + "exact_measurement_command is the only route to a figure.",
            PgIndexBloatSuppression.NegativeBloat =>
                "STRUCTURAL, not remediable: PostgreSQL 13 and above store duplicate keys once in a posting "
                + "list, so real storage is denser than per-tuple arithmetic predicts and a CORRECT model "
                + "still over-predicts. exact_measurement_command is the only route to a figure.",
            PgIndexBloatSuppression.NameTypedKey =>
                "STRUCTURAL, not remediable: a name-typed key column's pg_stats width is the padded 64 "
                + "bytes rather than the stored text, so the statistic exists and is the wrong quantity. "
                + "exact_measurement_command is the only route to a figure.",
            PgIndexBloatSuppression.NoPagesYet =>
                "Nothing to fix: these indexes occupy no pages, so there is no space in them to reclaim. "
                + "They are here because the trust predicate is the presence of a reason, and this is the "
                + "one reason that is a measurement rather than a gap.",
            _ =>
                "UNATTRIBUTED: the stored reason text is not one this build recognises, so its bytes are "
                + "counted in the suppressed total and its cause is not named. A collector newer or older "
                + "than this read produces exactly this; check the collector and this read are the same "
                + "build before reading anything else into it.",
        };

    /// <summary>
    /// The arm this server's coverage falls in, the population figures it was decided from, and the
    /// sentence for it.
    ///
    /// <para><b>The candidate total is DERIVED from the parts rather than passed in, and that is a
    /// correctness choice.</b> Every row is either trusted or suppressed — <c>skipped_reason IS NULL</c>
    /// partitions the population exhaustively — so a separately queried total could only ever disagree with
    /// the breakdown printed to explain it, and a census whose parts do not add up to its total is worse
    /// than one that omits the total.</para>
    ///
    /// <para>Order is load-bearing. <paramref name="evidenceCollectorRan"/> is asked FIRST because every
    /// other arm is an inference from figures that mean nothing until the collector supplying them has
    /// produced a run — asking it later lets an absent collector answer
    /// <see cref="PgIndexBloatCoverageArm.NoCandidates"/>, which is a confident all-clear over no
    /// measurement. Then the candidate count, because a server with nothing to model cannot have a coverage
    /// gap worth reporting. Only then the arms that need the trusted tally to tell apart.</para>
    /// </summary>
    /// <param name="evidenceCollectorRan">
    /// Whether <c>pg_index_bloat</c> recorded a SUCCEEDING run for this server in the evidence window.
    /// Deliberately a run and deliberately a succeeding one: a run that hit its command deadline stores
    /// nothing and still writes a log row, so an unfiltered probe would report "it ran" over zero rows and
    /// this classifier would answer <see cref="PgIndexBloatCoverageArm.NoCandidates"/> — "nothing to do" —
    /// on a target whose collector is timing out. Measured at 3 errored runs of 7 on a live target.
    /// </param>
    /// <param name="estimated">Candidates whose latest row in the window carries a statistics estimate.</param>
    /// <param name="exactlyMeasured">Candidates whose latest row is an older <c>pgstatindex</c> measurement
    /// still inside retention. Kept separate from <paramref name="estimated"/> because the two mean
    /// different things about how much the number can be trusted, not because they rank differently.</param>
    /// <param name="suppressed">One bucket per distinct stored reason. Merged by key and ranked by bytes
    /// here rather than at the call site, so two surfaces cannot rank them two ways.</param>
    /// <param name="returnedRows">Rows the caller's read actually returned — over the caller's window and
    /// subject to the caller's limit, which is why it is labelled as such and never divided into a
    /// population figure.</param>
    public static PgIndexBloatCoverageVerdict Classify(
        bool evidenceCollectorRan,
        PgIndexBloatTally estimated,
        PgIndexBloatTally exactlyMeasured,
        IReadOnlyList<PgIndexBloatSuppressionBucket>? suppressed,
        int returnedRows)
    {
        var buckets = Merge(suppressed);
        var trusted = estimated + exactlyMeasured;
        var candidates = buckets.Aggregate(
            trusted,
            (running, bucket) => running + new PgIndexBloatTally(bucket.IndexCount, bucket.IndexBytes));

        if (!evidenceCollectorRan)
        {
            return new PgIndexBloatCoverageVerdict(
                PgIndexBloatCoverageArm.Undetermined,
                Census(null, null, null, [], returnedRows),
                "WHAT SHARE of this server's indexes have an answer cannot be established: pg_index_bloat "
                + "recorded no succeeding run here in this window. It collects DAILY and only off a writer, "
                + "so a read replica, a window shorter than a cycle, and a collector erroring on every "
                + "cycle all legitimately leave no evidence - and this read will not guess which. Do not "
                + "read the rows beside this as a coverage claim in either direction.",
                default,
                default,
                default,
                []);
        }

        var census = Census(candidates, estimated, exactlyMeasured, buckets, returnedRows);

        if (candidates.IndexCount <= 0)
        {
            /* The RETURNED ROW COUNT is asked before the emptiness verdict is claimed, because the two are
               measured over different spans and can therefore disagree. Rows in hand say candidate indexes
               DID exist when they were collected, whatever the evidence window says about now - so "there
               is nothing here" is a statement the data printed beside it contradicts. */
            if (returnedRows > 0)
            {
                return new PgIndexBloatCoverageVerdict(
                    PgIndexBloatCoverageArm.EvidenceStale,
                    census,
                    "COVERAGE CANNOT BE ATTRIBUTED - the evidence and the rows describe different "
                    + "populations. Rows were returned, so candidate indexes DID exist when they were "
                    + "collected, yet the evidence window holds none. The two are measured over different "
                    + "spans, so an index dropped since, a database gone from the enumeration, or a "
                    + "collector erroring for longer than the evidence window produces exactly this. Read "
                    + "the rows as describing indexes that may no longer exist, and do not read this as "
                    + "full coverage or as an absence of bloat.",
                    candidates,
                    estimated,
                    exactlyMeasured,
                    buckets);
            }

            return new PgIndexBloatCoverageVerdict(
                PgIndexBloatCoverageArm.NoCandidates,
                census,
                "NO CANDIDATES: the collector ran and this server has no btree index for it to model. This "
                + "census has no size floor, so that is a genuinely empty schema rather than a floor "
                + "filtering small indexes out. Nothing to fix.",
                candidates,
                estimated,
                exactlyMeasured,
                buckets);
        }

        if (trusted.IndexCount <= 0)
        {
            return new PgIndexBloatCoverageVerdict(
                PgIndexBloatCoverageArm.NothingTrusted,
                census,
                "NO COVERAGE AT ALL: every candidate index on this server carries a reason instead of an "
                + "answer, so nothing here ranks anything. A page of rows from this server is 100% "
                + "suppressed because that is ALL there is, not because the limit truncated a ranking - and "
                + "raising the limit cannot change it. The largest bucket by BYTES, which is the one worth "
                + "acting on: " + Dominant(buckets),
                candidates,
                estimated,
                exactlyMeasured,
                buckets);
        }

        if (buckets.Count > 0)
        {
            return new PgIndexBloatCoverageVerdict(
                PgIndexBloatCoverageArm.PartialCoverage,
                census,
                "PARTIAL COVERAGE, not a clean bill of health. Indexes with no answer are ABSENT from the "
                + "ranking rather than unremarkable in it, and they sort FIRST, so a truncated read shows "
                + "them and not the answers behind them. Judge the gap by the byte share above and never by "
                + "the row share - the two rank these buckets differently. The largest bucket by BYTES: "
                + Dominant(buckets),
                candidates,
                estimated,
                exactlyMeasured,
                buckets);
        }

        return new PgIndexBloatCoverageVerdict(
            PgIndexBloatCoverageArm.FullyTrusted,
            census,
            "FULL COVERAGE: every candidate index on this server has an answer, with none withheld by a "
            + "modelling limit or a missing grant. The ranking describes the whole of this server's btree "
            + "footprint.",
            candidates,
            estimated,
            exactlyMeasured,
            buckets);
    }

    /// <summary>
    /// The verdict when the evidence read itself failed — <see cref="PgIndexBloatCoverageArm.Undetermined"/>
    /// with its OWN wording, because "the monitoring store could not be read" and "this collector does not
    /// run on this target" are different situations and only one of them is the design working.
    ///
    /// <para>Here rather than at the call site so every operator-facing sentence about this collector's
    /// coverage lives in one file and is pinned together. A caller composing its own would be the second
    /// copy, and the second copy is the one that drifts.</para>
    /// </summary>
    public static PgIndexBloatCoverageVerdict EvidenceUnreadable(int returnedRows) =>
        new(PgIndexBloatCoverageArm.Undetermined,
            Census(null, null, null, [], returnedRows),
            "WHAT SHARE of this server's indexes have an answer cannot be established: the monitoring store "
            + "read that supplies the figures above FAILED. That is a monitoring-side fault rather than "
            + "anything about the target, so check collection health for THIS store - and read nothing "
            + "about the target's indexes into it either way.",
            default,
            default,
            default,
            []);

    /// <summary>
    /// The buckets merged by key and ranked by BYTES descending — the ranking the whole issue turns on.
    /// Merged because two distinct stored prose strings can map to one key, and two entries under one key
    /// would let a reader add a bucket to itself.
    /// </summary>
    private static IReadOnlyList<PgIndexBloatSuppressionBucket> Merge(
        IReadOnlyList<PgIndexBloatSuppressionBucket>? suppressed)
    {
        if (suppressed is null || suppressed.Count == 0)
        {
            return [];
        }

        return
        [
            .. suppressed
                .GroupBy(bucket => bucket.Reason)
                .Select(group => new PgIndexBloatSuppressionBucket(
                    group.Key,
                    group.Sum(bucket => bucket.IndexCount),
                    group.Sum(bucket => bucket.IndexBytes),
                    group.Select(bucket => bucket.Detail).FirstOrDefault(detail => detail is not null)))
                /* Bytes, then count, then the key itself - a total order, so the dominant bucket a verdict
                   names is the same one on every call rather than whatever the grouping happened to emit. */
                .OrderByDescending(bucket => bucket.IndexBytes)
                .ThenByDescending(bucket => bucket.IndexCount)
                .ThenBy(bucket => bucket.Reason),
        ];
    }

    /// <summary>
    /// The largest bucket by bytes, named with its remedy and no figures. Its FIGURES are in the census;
    /// repeating them here would make the cause a function of the numbers rather than of the verdict.
    /// </summary>
    private static string Dominant(IReadOnlyList<PgIndexBloatSuppressionBucket> buckets) =>
        buckets.Count == 0
            ? string.Empty
            : Label(buckets[0].Reason) + ". " + Remedy(buckets[0].Reason);

    /// <summary>
    /// One bucket's short operator-facing label. Distinct from the enum name, which is a programming
    /// identifier, and from the collector's stored prose, which is a paragraph.
    /// </summary>
    public static string Label(PgIndexBloatSuppression reason) =>
        reason switch
        {
            PgIndexBloatSuppression.ColumnWidthsNotVisible => "column widths not visible for every key",
            PgIndexBloatSuppression.ParentNeverAnalyzed => "parent table never analyzed",
            PgIndexBloatSuppression.Partial => "partial index",
            PgIndexBloatSuppression.NegativeBloat => "negative bloat (deduplicated index)",
            PgIndexBloatSuppression.NameTypedKey => "name-typed key column",
            PgIndexBloatSuppression.NoPagesYet => "index occupies no pages",
            _ => "reason not recognised by this build",
        };

    /// <summary>
    /// The population figures and the returned row count, labelled, in the one format every arm uses. Nulls
    /// render as <see cref="NotMeasured"/>.
    ///
    /// <para>Every figure is formatted invariantly: the viewer runs on whatever desktop the operator has and
    /// the MCP answer is parsed by machines, so a separator that moved with the host locale would render
    /// one reading two ways.</para>
    /// </summary>
    private static string Census(
        PgIndexBloatTally? candidates,
        PgIndexBloatTally? estimated,
        PgIndexBloatTally? exactlyMeasured,
        IReadOnlyList<PgIndexBloatSuppressionBucket> buckets,
        int returnedRows)
    {
        var trusted = candidates is null || estimated is null || exactlyMeasured is null
            ? (PgIndexBloatTally?)null
            : estimated.Value + exactlyMeasured.Value;

        var suppressedCount = candidates is null
            ? (long?)null
            : candidates.Value.IndexCount - (trusted?.IndexCount ?? 0);
        var suppressedBytes = candidates is null
            ? (long?)null
            : candidates.Value.IndexBytes - (trusted?.IndexBytes ?? 0);

        return "Candidate btree indexes on this server (" + EvidenceHoursDescription
            + ", independently of your row limit): " + Tally(candidates)
            + ". WITH a trusted answer: " + Tally(trusted) + " - " + BytePercent(trusted, candidates)
            + " (from the statistics model: " + Tally(estimated)
            + "; older exact measurements still in retention: " + Tally(exactlyMeasured)
            + "). WITH NO answer: " + Count(suppressedCount) + " holding " + Bytes(suppressedBytes)
            + ", by reason and ranked BY BYTES because the row ranking and the byte ranking disagree"
            + Breakdown(buckets)
            + ". Rows returned (over the window you asked for, and capped by your row limit): "
            + Count(returnedRows) + ".";
    }

    private static string Breakdown(IReadOnlyList<PgIndexBloatSuppressionBucket> buckets) =>
        buckets.Count == 0
            ? string.Empty
            : " - " + string.Join(
                "; ",
                buckets.Select(bucket =>
                    Label(bucket.Reason) + " " + Count(bucket.IndexCount) + " / " + Bytes(bucket.IndexBytes)));

    private static string Tally(PgIndexBloatTally? tally) =>
        tally is { } present
            ? Count(present.IndexCount) + " holding " + Bytes(present.IndexBytes)
            : NotMeasured;

    /// <summary>
    /// The BYTE-weighted coverage share, which is the figure the row share gets wrong. Measured on the
    /// fleet, 917 trusted rows of 7,597 reads as 12% by rows and is 3.84% of the footprint, because the
    /// suppressed indexes are the big ones.
    ///
    /// <para>Three outcomes, not two. A zero denominator over a MEASURED population is undefined arithmetic
    /// rather than an absent measurement, so it answers <see cref="NotApplicable"/> — putting
    /// <see cref="NotMeasured"/> there would print the no-evidence word on the one arm that has evidence,
    /// which is this whole defect class reappearing inside its own fix.</para>
    /// </summary>
    private static string BytePercent(PgIndexBloatTally? trusted, PgIndexBloatTally? candidates)
    {
        if (trusted is not { } part || candidates is not { } whole)
        {
            return NotMeasured + " as a share of the candidate footprint";
        }

        return whole.IndexBytes > 0
            ? (100.0 * part.IndexBytes / whole.IndexBytes).ToString("0.00", CultureInfo.InvariantCulture)
              + "% of the candidate footprint"
            : NotApplicable + " as a share of the candidate footprint, which is zero bytes";
    }

    private static string Count(long? value) =>
        value is { } present ? present.ToString("N0", CultureInfo.InvariantCulture) : NotMeasured;

    /// <summary>
    /// Bytes in a unit a person can read, chosen per figure. A fixed unit cannot serve both ends of this
    /// range: the fleet's largest suppression bucket is 4,144 GB and its smallest is 22 MB, and rendering
    /// the small one in the large one's unit prints a ZERO beside a non-zero index count — which is the
    /// exact confusion between "none" and "a small amount" this class exists to prevent. Comparison across
    /// buckets is served by the ORDERING, which is already done for the reader, so it does not need one
    /// unit.
    /// </summary>
    private static string Bytes(long? value)
    {
        if (value is not { } bytes)
        {
            return NotMeasured;
        }

        const long Kib = 1024L;
        const long Mib = Kib * 1024L;
        const long Gib = Mib * 1024L;

        return bytes switch
        {
            < Kib => bytes.ToString("N0", CultureInfo.InvariantCulture) + " B",
            < Mib => (bytes / (double)Kib).ToString("N1", CultureInfo.InvariantCulture) + " KB",
            < Gib => (bytes / (double)Mib).ToString("N1", CultureInfo.InvariantCulture) + " MB",
            _ => (bytes / (double)Gib).ToString("N1", CultureInfo.InvariantCulture) + " GB",
        };
    }
}
