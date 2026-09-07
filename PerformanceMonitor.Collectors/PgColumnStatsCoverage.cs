/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Which of <see cref="PgColumnStatsCollector"/>'s outcomes produced the row count a read is looking at
/// (#3154). Named arms rather than a boolean, because the whole defect was that a zero row count arrived
/// with no way to tell four different situations apart.
/// </summary>
public enum PgColumnStatsCoverageArm
{
    /// <summary>
    /// No per-table evidence for this server in the window, so no arm can be selected. This is the arm that
    /// keeps the others honest: without it, "the evidence collector never ran here" and "nothing on this
    /// target is large enough" both present as zero candidate tables, and only one of them is a diagnosis.
    /// </summary>
    Undetermined,

    /// <summary>
    /// Nothing on the target reaches <see cref="PgColumnStatsCollector.MinimumRelPages"/>. The collector had
    /// nothing to read, the empty is correct, and there is nothing for an operator to fix.
    /// </summary>
    BelowSizeFloor,

    /// <summary>
    /// Tables clear the floor and not one of them has column statistics this monitoring login can read.
    /// <c>pg_stats</c> filters on <c>has_column_privilege</c>, so a login without SELECT gets an empty view
    /// and no error at all — the arm that is measured on a managed fleet with a restricted login, and the
    /// only arm carrying a remedy.
    /// </summary>
    StatisticsNotVisible,

    /// <summary>
    /// Tables clear the floor, statistics on some of them ARE readable by this login, and the collector still
    /// stored nothing. Neither legitimate cause is in force, so this is a collection fault. Today's read
    /// reports it identically to the two legitimate arms, which is how a broken query would go unnoticed.
    /// </summary>
    CollectionFault,

    /// <summary>
    /// Statistics were stored, and they describe FEWER tables than clear the floor. A ranking over a partial
    /// view is not a ranking over the target — the same rule #3114 established for a measurement cap, one
    /// collector over.
    /// </summary>
    PartialVisibility,

    /// <summary>Statistics were stored, and they cover every table that clears the floor.</summary>
    FullyMeasured,
}

/// <summary>
/// One <see cref="PgColumnStatsCoverageArm"/>, the figures it was decided from, and the verdict sentence —
/// as one value, so a caller cannot print the arm without its explanation or the explanation without the
/// counts it rests on.
///
/// <para><b><see cref="Census"/> and <see cref="Cause"/> are separate on purpose, and it is not cosmetic.</b>
/// The census is the two counts, in one format for every arm; the cause is the verdict, and it carries NO
/// figures at all. A guard that the arms are distinguishable has to compare the CAUSE — comparing the whole
/// message would pass on the counts differing, which they always do (the floor arm needs zero candidate
/// tables and the privilege arm needs some), so a pin over the composed string would report discrimination
/// this class had stopped providing.</para>
/// </summary>
/// <param name="Census">The figures, labelled, in the same shape whatever the arm — so two servers'
/// answers are comparable and an operator reading one arm knows what the next arm's numbers would mean.</param>
/// <param name="Cause">The verdict and its remedy, carrying no counts.</param>
public readonly record struct PgColumnStatsCoverageVerdict(
    PgColumnStatsCoverageArm Arm,
    string Census,
    string Cause)
{
    /// <summary>The census then the cause, for the surfaces that print one string.</summary>
    public string Message => Census + " " + Cause;
}

/// <summary>
/// Which of <c>pg_column_stats</c>' outcomes produced a given row count, and the one sentence that says so
/// (#3154).
///
/// <para><b>The defect this closes.</b> <see cref="PgColumnStatsCollector"/> documents two legitimate ways
/// to return nothing — the size floor and <c>pg_stats</c>' privilege filter — and distinguishes them in its
/// documentation and not in its output. Read read-only against a 50-target Aurora store on 2026-09-07:
/// 553 SUCCESS runs since that store was built, ONE distinct status, zero rows stored ever, 299 ms at its
/// slowest, and a NULL note on 99 of the last 100 runs. The reads then recited BOTH causes in one message
/// and selected neither, which is prose about the mechanism rather than a diagnosis of it — an operator
/// could not tell which thing to go and do.</para>
///
/// <para><b>Where the evidence comes from, and why it is not a new probe.</b> Both counts are already in the
/// store, collected by <c>pg_table_bloat_stats</c>: it measures every table at or above 1 MB and carries
/// <c>estimate_unavailable</c>, which is TRUE whenever a table has any column with no <c>pg_stats</c> row —
/// the identical filter, on the identical view, from the identical login. So the arm is answered at READ
/// time from collected rows, the way <c>CollectorRuntimePrecondition</c> answers a precondition and the way
/// <c>get_pg_blocking</c> gets its capture denominator. No monitored server is touched to produce it.</para>
///
/// <para><b>What each direction of the evidence actually proves.</b> <c>estimate_unavailable = false</c> is
/// a SUFFICIENT condition for visibility: the flag cannot be false unless every column of that table had a
/// <c>pg_stats</c> row this login could read. That is the strong direction and it is the one
/// <see cref="PgColumnStatsCoverageArm.CollectionFault"/> rests on — a readable table plus a stored nothing
/// is a fault, with no privilege explanation available. The other direction is weaker: the flag is also set
/// by a <c>name</c>-typed column and by <c>reltuples &lt; 0</c>, so a table with no confirmed-readable
/// statistics is not by itself proof of a denied grant. Against that same store on 2026-09-07 those arms
/// were negligible — 2 of 60,202 rows in 48 hours carried <c>reltuples &lt; 0</c>, and 47,960 had been
/// analyzed at some point, so the statistics exist on the targets — so
/// <see cref="PgColumnStatsCoverageArm.StatisticsNotVisible"/> names the privilege filter as the cause and
/// says "confirmed readable" rather than claiming a certainty the flag cannot carry.</para>
///
/// <para><b>Pure policy, no clock and no I/O.</b> The caller reads the counts and passes them in, the same
/// discipline as <c>RollingCountAlertGate</c> — which is what lets the arm selection be asserted without a
/// store, and what lets one classifier serve the MCP tool and the WPF panel so the two cannot disagree
/// about what an empty means.</para>
/// </summary>
public static class PgColumnStatsCoverage
{
    /// <summary>
    /// The remedy sentence, one copy. It names the PostgreSQL 14+ role because that is the grant measured to
    /// restore byte-identical agreement with a superuser's numbers (#2542), and it names the explicit GRANT
    /// beside it because 13 has no such role.
    /// </summary>
    public const string PrivilegeRemedy =
        "Remedy: grant the monitoring login read access to the tables - GRANT pg_read_all_data TO <login> on "
        + "PostgreSQL 14 and above, or GRANT SELECT on the tables themselves on 13, both verified to restore "
        + "the statistics in full. Row-level security empties the same view by the same filter, so check for "
        + "policies on these tables if the grant is already in place.";

    /// <summary>
    /// What the census prints where a count should be, when there is no measurement to put there. A word,
    /// never a zero: "0 tables above the floor" is a finding and "we did not look" is not, and this whole
    /// class exists because those two were rendered identically.
    /// </summary>
    public const string NotMeasured = "not measured";

    /// <summary>
    /// The arm that produced <paramref name="storedColumnRows"/>, the counts it was decided from, and the
    /// sentence for it.
    ///
    /// <para>Order is load-bearing. <paramref name="evidenceRuns"/> is asked FIRST because every other arm
    /// is an inference from counts that only mean something once the collector supplying them has run —
    /// asking it later would let an absent evidence collector answer
    /// <see cref="PgColumnStatsCoverageArm.BelowSizeFloor"/>, which is a confident all-clear built on no
    /// measurement. Then the floor, because a target with nothing to read cannot have a privilege problem
    /// worth reporting. Only then the two arms that need the visible count to tell apart.</para>
    /// </summary>
    /// <param name="evidenceRuns">
    /// <c>pg_table_bloat_stats</c> runs recorded for this server in the window. Zero means no evidence, and
    /// it is deliberately a RUN count rather than a row count: a run that stored no rows is the measurement
    /// that establishes <see cref="PgColumnStatsCoverageArm.BelowSizeFloor"/>, so collapsing the two would
    /// throw away the only reading that distinguishes it from an absent collector.
    /// </param>
    /// <param name="candidateTables">
    /// Distinct tables whose latest measurement in the window is at or above
    /// <see cref="PgColumnStatsCollector.MinimumRelPages"/> - what the collector would have read.
    /// </param>
    /// <param name="tablesWithVisibleStatistics">
    /// How many of <paramref name="candidateTables"/> had every column's <c>pg_stats</c> row readable by
    /// this login (<c>estimate_unavailable = false</c>).
    /// </param>
    /// <param name="storedColumnRows">Column statistic rows the read actually returned.</param>
    public static PgColumnStatsCoverageVerdict Classify(
        int evidenceRuns,
        int candidateTables,
        int tablesWithVisibleStatistics,
        int storedColumnRows)
    {
        if (evidenceRuns <= 0)
        {
            return new PgColumnStatsCoverageVerdict(
                PgColumnStatsCoverageArm.Undetermined,
                Census(null, null, storedColumnRows),
                "WHICH cause produced this cannot be established. pg_table_bloat_stats supplies both counts "
                + "above and recorded no run for this server in this window; it collects on WRITERS only "
                + "and hourly, so a read replica - where pg_column_stats does run - and a window shorter "
                + "than an hour both legitimately have no evidence here. So this read will not say whether "
                + "the size floor, the pg_stats privilege filter or a collection fault is responsible, and "
                + "it will not guess.");
        }

        var census = Census(candidateTables, tablesWithVisibleStatistics, storedColumnRows);

        if (candidateTables <= 0)
        {
            return new PgColumnStatsCoverageVerdict(
                PgColumnStatsCoverageArm.BelowSizeFloor,
                census,
                "CAUSE: the size floor. No table on this server is large enough for this collector to read, "
                + "so there was nothing to collect - statistics on a table that small cannot produce a "
                + "misestimate worth reading. Not a privilege problem, and nothing to fix.");
        }

        if (storedColumnRows <= 0)
        {
            if (tablesWithVisibleStatistics <= 0)
            {
                return new PgColumnStatsCoverageVerdict(
                    PgColumnStatsCoverageArm.StatisticsNotVisible,
                    census,
                    "CAUSE: the privilege filter, not the size floor. Tables clear the floor and not one of "
                    + "them has column statistics confirmed readable by this monitoring login, so pg_stats - "
                    + "a view over pg_statistic filtered by has_column_privilege - returned an empty result "
                    + "and no error at all. The statistics exist on the target; this login cannot see them. "
                    + PrivilegeRemedy);
            }

            return new PgColumnStatsCoverageVerdict(
                PgColumnStatsCoverageArm.CollectionFault,
                census,
                "CAUSE: neither legitimate one - this is a COLLECTION FAULT. Tables clear the floor AND some "
                + "of them have column statistics this login CAN read, and nothing was stored anyway. The "
                + "floor does not explain it and the privilege filter does not explain it, so the collector "
                + "or its query is wrong. Raise it rather than reading the empty as healthy statistics.");
        }

        if (tablesWithVisibleStatistics < candidateTables)
        {
            return new PgColumnStatsCoverageVerdict(
                PgColumnStatsCoverageArm.PartialVisibility,
                census,
                "PARTIAL COVERAGE, not a clean bill of health. Fewer tables are represented here than clear "
                + "the floor, and the rest have column statistics this monitoring login cannot read - so "
                + "their columns are ABSENT from the ranking rather than unremarkable in it. "
                + PrivilegeRemedy);
        }

        return new PgColumnStatsCoverageVerdict(
            PgColumnStatsCoverageArm.FullyMeasured,
            census,
            "FULL COVERAGE: every table above the floor is represented, with none withheld by the pg_stats "
            + "privilege filter. The ranking describes the whole of what this collector reads.");
    }

    /// <summary>
    /// The verdict when the evidence read itself failed — <see cref="PgColumnStatsCoverageArm.Undetermined"/>
    /// with its own wording, because "the store could not be read" and "the evidence collector does not run
    /// here" are different situations and only one of them is the design working.
    ///
    /// <para>Here rather than at the call site so every operator-facing sentence about this collector's
    /// coverage lives in one file and is pinned together. A caller that composed its own would be the second
    /// copy, and the second copy is the one that drifts.</para>
    /// </summary>
    public static PgColumnStatsCoverageVerdict EvidenceUnreadable(int storedColumnRows) =>
        new(PgColumnStatsCoverageArm.Undetermined,
            Census(null, null, storedColumnRows),
            "WHICH cause produced this cannot be established: the monitoring store read that supplies the "
            + "counts above FAILED. That is a monitoring-side fault rather than anything about the target, so "
            + "check collection health for THIS store - and read nothing about the target's statistics into "
            + "it either way.");

    /// <summary>
    /// The two counts and the row count, labelled, in the one format every arm uses. Nulls render as
    /// <see cref="NotMeasured"/>.
    ///
    /// <para>The floor is quoted from <see cref="PgColumnStatsCollector.MinimumRelPages"/> rather than
    /// retyped: the number an operator is told about has to be the number the shipped query filters on.
    /// Every figure is formatted invariantly - the viewer runs on whatever desktop the operator has and the
    /// MCP answer is parsed by machines, so a thousands separator that moved with the host locale would
    /// render one reading two ways.</para>
    /// </summary>
    private static string Census(int? candidateTables, int? tablesWithVisibleStatistics, int storedColumnRows) =>
        "Tables at or above the " + Figure(PgColumnStatsCollector.MinimumRelPages) + " page floor: "
        + Figure(candidateTables) + ". Of those, with column statistics this monitoring login can read: "
        + Figure(tablesWithVisibleStatistics) + ". Column statistic rows returned: "
        + Figure(storedColumnRows) + ".";

    private static string Figure(long? value) =>
        value is { } present ? present.ToString("N0", CultureInfo.InvariantCulture) : NotMeasured;
}
