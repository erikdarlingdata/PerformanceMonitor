/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads measured b-tree index bloat (<c>pg_index_bloat</c>, #2561) — the latest MEASUREMENT per index
/// within the window, ranked by RECLAIMABLE BYTES rather than by density.
///
/// <para><b>The latest measurement, not the latest row (#3153).</b> The collector rotates which indexes it
/// measures, so every index has a row on every cycle and most of them are labels rather than measurements.
/// Keeping the newest row per index would hand back the newest LABEL and discard a real measurement a few
/// cycles back in the same window — so an index measured on Monday would read as unmeasured on Tuesday.
/// <see cref="PgIndexBloatRow.CaptureTime"/> is therefore when the index was MEASURED, which can predate
/// the last collection by most of a pass.</para>
///
/// <para><b>Ranking by density would be wrong, and this is the whole reason the read exists.</b> A tiny
/// index at 40% density is worse-looking and worth nothing; a large one at 70% is where the space actually
/// is. The estimate of what a <c>REINDEX</c> would return is <c>index_bytes</c> scaled by how far density
/// sits below what a freshly built index of that shape achieves — and since a healthy index measures near
/// 90 rather than 100, the shortfall is computed against 90, not against a full page.</para>
///
/// <para><b>That 90 is a floor, not a constant, and the read says so.</b> Measured across freshly built
/// indexes the value ranged 89.98–91.48, and post-<c>REINDEX</c> 87.07–90.81. So the reclaimable figure is
/// an ESTIMATE derived from a measurement, and it is presented beside the raw density rather than replacing
/// it. Anyone acting on it should confirm against the index's own post-rebuild density.</para>
///
/// <para><b>Skipped indexes sort to the top, not out of sight.</b> An index too large to measure is exactly
/// the one most likely to be holding reclaimable space, so a read that filtered it out would hide the
/// biggest candidate behind a performance optimisation.</para>
///
/// <para><b>An EMPTY index has no density, and it still gets a row (#3121).</b> <c>pgstatindex</c> returns
/// NaN for both float columns when there are no leaf pages to average over, so the read normalises NaN to
/// NULL and reports 0 reclaimable — the index is measured, and what it holds is nothing. Filtering the row
/// out instead would serve the read successfully while hiding a real index, which is the failure mode that
/// looks most like a fix.</para>
///
/// <para><b>The row list cannot carry its own denominator, so a second read supplies it (#3278).</b> The
/// unmeasured-first sort above means any <c>LIMIT</c> smaller than the suppressed population returns 100%
/// suppressed rows — structurally, not by chance — and raising the limit does not help while the suppressed
/// population is still larger. <see cref="GetCoverageVerdictAsync"/> answers what share of the server has an
/// answer with its own population-level query, so no page of this read can be mistaken for a coverage claim.
/// It reports BYTES beside every count, because on the live fleet the row ranking and the byte ranking of the
/// suppression buckets disagree.</para>
///
/// <para>Shared by the WPF tab and the MCP surface so there is one copy of this SQL, per #2530.</para>
/// </summary>
public static class DarlingPgIndexBloatReader
{
    /// <param name="AvgLeafDensity">The server's raw figure. NULL when the index was skipped, and NULL when
    /// the index is EMPTY — <c>pgstatindex</c> divides by a leaf-page count of zero and returns NaN, which
    /// is not a density and cannot be carried to any consumer here (see the SQL's own note).</param>
    /// <param name="LeafFragmentation">As above, and NaN on an empty index for the same reason.</param>
    /// <param name="EstimatedReclaimableBytes">What a rebuild might return, derived from the density
    /// shortfall against a 90% healthy floor. NULL when not measured, and 0 both when the index is at or
    /// above that floor and when it is empty — an index with no leaf pages holds nothing to reclaim, which
    /// is a measurement rather than an absence of one.</param>
    /// <param name="SkippedReason">Non-null means this index was NOT measured — its bloat is unknown rather
    /// than zero. An empty index leaves this NULL: it was measured, and the measurement is that it is
    /// empty. Non-null here now means the WINDOW holds no measurement of this index at all, not merely that
    /// the last cycle passed over it (#3153).</param>
    /// <param name="CaptureTime">When the row this read kept was collected — so on a measured row, when the
    /// index was last MEASURED. Under rotation that is not "the last cycle": it can be most of a pass old,
    /// and a consumer showing the density owes the reader this timestamp beside it.</param>
    public sealed record PgIndexBloatRow(
        string? DatabaseName,
        string? SchemaName,
        string? TableName,
        string? IndexName,
        long IndexBytes,
        int? TreeLevel,
        long? EmptyPages,
        long? DeletedPages,
        double? AvgLeafDensity,
        double? LeafFragmentation,
        long? EstimatedReclaimableBytes,
        string? SkippedReason,
        /// <summary><c>estimated</c> or <c>measured</c> — this row's PROVENANCE, not its outcome.</summary>
        string? MeasurementKind,
        double? EstBloatPct,
        long? IndexPages,
        long? TableRows,
        int? Fillfactor,
        long? EstTupleBytes,
        long? EstLeafPages,
        bool? PgstattupleAvailable,
        DateTime CaptureTime)
    {
        /// <summary>
        /// True when this row came from the statistics estimate rather than from <c>pgstatindex</c>.
        /// </summary>
        public bool IsEstimate =>
            string.Equals(MeasurementKind, "estimated", StringComparison.Ordinal);

        /// <summary>
        /// When this index was EXACTLY measured, or null when the window holds no exact measurement of it.
        ///
        /// <para>Not the same as <see cref="CaptureTime"/>, and the difference is the point (#3153): on a
        /// labelled row <see cref="CaptureTime"/> is the LABEL's own timestamp, and showing that in a
        /// "measured" column would read as the age of a measurement that does not exist. Defined here so
        /// the grid and the MCP payload cannot disagree about which rows have an age at all.</para>
        ///
        /// <para><b>#3234 narrowed this, and the narrowing is load-bearing.</b> A row with no
        /// <see cref="SkippedReason"/> used to mean measured, because a census was the only thing writing
        /// rows. It now usually means ESTIMATED. Left as it was, this property would have reported an
        /// estimate's timestamp as the age of a measurement that never happened — an accurate-looking
        /// figure for the wrong quantity, on the surface an operator uses to decide whether to trust the
        /// number. So it tests the row's provenance and not merely the absence of a reason.</para>
        /// </summary>
        public DateTime? MeasuredAt =>
            SkippedReason is null && !IsEstimate ? CaptureTime : null;

        /// <summary>
        /// When this index was ESTIMATED, or null when this row is not a successful estimate. The
        /// counterpart to <see cref="MeasuredAt"/>: exactly one of the two is non-null on a row that
        /// carries an answer, and neither is on a row that carries a reason.
        /// </summary>
        public DateTime? EstimatedAt =>
            SkippedReason is null && IsEstimate ? CaptureTime : null;

        /// <summary>
        /// The exact <c>pgstatindex</c> call for this index, prefixed with a <c>CREATE EXTENSION</c> when
        /// pgstattuple is not installed on the database this row came from.
        ///
        /// <para><b>Defined here rather than at each surface</b> so the grid and the MCP payload cannot
        /// disagree about the command they tell an operator to run — one expression cannot drift from
        /// itself. Built from schema and index name and never from the collected oid, which does not
        /// survive a dump and restore and would send the reader at whatever now holds that number.</para>
        ///
        /// <para>It is offered on EVERY row, including ones that estimated cleanly: the estimate is close
        /// enough to decide which index to act on and not close enough to justify a REINDEX, so the exact
        /// figure is the next step for any index somebody intends to touch. On a partial or deduplicated
        /// index it is not an escalation but the ONLY route — no statistics model reaches those.</para>
        /// </summary>
        public string ExactMeasurementCommand
        {
            get
            {
                /* TWO levels of escaping, and both are load-bearing rather than defensive.
                   pgstatindex takes regclass, whose TEXT input parses like an SQL identifier: an unquoted
                   part is folded to lower case, so a camelCase or reserved-word index resolves to the wrong
                   object or to none. And a name created through a double-quoted CREATE INDEX may contain
                   almost any character INCLUDING a single quote, which would close the outer literal and
                   append whatever follows to a command an operator is being invited to paste into a
                   privileged session. So each part is quoted as an identifier (doubling any embedded
                   double quote, which is quote_ident's rule) and the assembled name is then escaped for
                   the string literal it sits inside (doubling any single quote). */
                var literal = PgIdentifier.Qualify(SchemaName, IndexName);

                return PgstattupleAvailable == false
                    ? $"CREATE EXTENSION pgstattuple; SELECT * FROM pgstatindex('{literal}');"
                    : $"SELECT * FROM pgstatindex('{literal}');";
            }
        }
    }

    /* DISTINCT ON the index identity gives one row per index in one pass. The outer ORDER BY then ranks
       for reading, which the inner one cannot do.

       THE MEASURED ROW WINS, and only then the newest (#3153). Since the collector rotates which indexes it
       measures, every index gets a row on every cycle and most of those rows are LABELS - so plain
       collection_time DESC returns the newest LABEL and throws away a real measurement sitting a few days
       back in the same window. Measured on a two-cycle store: an index measured at 72.5% density on day one
       came back on day two with no density at all and a rotation-cursor reason, because day two's row was
       newer. That turns the whole rotation mechanism into something an operator cannot see - the read would
       only ever show the LAST cycle's measured set, which is what it showed before rotation existed.

       So the row kept per index is the newest MEASUREMENT if the window holds one, and the newest label
       otherwise - an index the window never measured still arrives with its reason, and still sorts to the
       top. CaptureTime therefore means "when this index was last MEASURED" on a measured row, and it can
       legitimately predate the last collection by most of a pass; the surfaces state that, because a density
       presented without its age is a claim about now that it is not.

       The whole row travels together on purpose. estimated_reclaimable_bytes is index_bytes scaled by the
       density shortfall, so pairing today's size with an older density would compute a figure that describes
       no state the index was ever in. Taking the measured row keeps size, density and estimate internally
       consistent, at the cost of showing a size that may have grown since - which the timestamp discloses.

       database_name leads the distinct key (#2599) - this collector runs once per database, and an index
       name is only unique within one, so without it the newest collection_time silently picks which
       database's copy of a shared schema the grid shows.

       The reclaimable estimate uses GREATEST(0, ...) so an index measuring ABOVE the healthy floor reports
       zero rather than a negative saving - which is a real case, since freshly built indexes measured up to
       91.48.

       NULLIF guards the division: a density of 0 is not something pgstatindex returns for a live index, but
       a divide-by-zero would turn one odd row into a failed read for the whole grid.

       THE THIRD STATE OF A FLOAT COLUMN (#3121). pgstatindex reports avg_leaf_density and
       leaf_fragmentation as NaN for an EMPTY b-tree index - verified on PostgreSQL 18.6 against a real index
       on a table with no rows, which is an entirely ordinary object (a fresh partition, a table whose rows
       were all deleted, a constraint index on an unpopulated table). avg_leaf_density is double precision in
       the store, so NaN is representable, persists, and is re-read on every later call. An IS NULL guard
       alone is not enough here, and it is not enough for a reason its own vocabulary hides: "handle the
       missing case" covers two of the three states a float column has, and a NaN that reaches the ::bigint
       cast below raises 22003, which fails the WHOLE read rather than the row.

       NULLIF(x, 'NaN'::double precision) is the whole normalisation, and it turns on a PostgreSQL semantic
       that is the opposite of the C one: NaN compares EQUAL to itself here, so nullif catches it and
       IS NOT DISTINCT FROM would too, while an IEEE-style self-inequality check (x <> x) is false and
       catches nothing. It is identity on a real density and on NULL, so the two states that already worked
       are untouched.

       Normalising is not optional decoration on the density columns themselves: a NaN that clears the SQL
       still reaches System.Text.Json, whose default number handling REJECTS NaN, so it would take down the
       MCP read and the web page one layer up from the cast. Nothing may leave this read as NaN.

       AN EMPTY INDEX REPORTS 0 RECLAIMABLE, NOT NULL. NULL in this column means "not measured" - that is
       what a skipped row carries, and the read hoists those to the top on the strength of it. An empty
       index WAS measured, and what was measured is that it holds nothing to reclaim, so it takes the same 0
       an above-floor index takes and ranks where a 0 belongs. Reporting NULL would sort it among the
       unmeasured and claim its bloat was unknown when it is known to be nil.

       THE ESTIMATE IS COMPUTED ONCE, in the inner scope, and the outer projection and the outer ORDER BY
       both reference that one column. Two copies of the expression would be a correctness hazard rather
       than a duplication: they have to agree, and the failure of a disagreement is the ordering raising
       while the projection succeeds - a read that half works, for a reason no reader could see. One
       expression cannot drift from itself. */
    public const string PgIndexBloatSql = """
        SELECT database_name, schema_name, table_name, index_name, index_bytes, tree_level,
               empty_pages, deleted_pages,
               NULLIF(avg_leaf_density, 'NaN'::double precision) AS avg_leaf_density,
               NULLIF(leaf_fragmentation, 'NaN'::double precision) AS leaf_fragmentation,
               estimated_reclaimable_bytes,
               skipped_reason,
               measurement_kind,
               est_bloat_pct, index_pages, table_rows, fillfactor, est_tuple_bytes, est_leaf_pages,
               pgstattuple_available,
               collection_time
        FROM (
            SELECT DISTINCT ON (database_name, schema_name, table_name, index_name)
                   database_name, schema_name, table_name, index_name, index_bytes, tree_level,
                   empty_pages, deleted_pages, avg_leaf_density, leaf_fragmentation,
                   /* ONE expression serving two row kinds. A row written since #3234 carries the
                      collector's own est_reclaimable_bytes; the store also holds 90 days of exact
                      pgstatindex rows written before it, which carry a density instead. COALESCE picks
                      whichever exists, and it is correct in all three states rather than by luck: an
                      estimated row that PASSED its gate has the stored figure, an estimated row that was
                      suppressed has NULL on both sides and stays NULL because unknown is not zero, and a
                      historical measured row falls through to the density derivation below. */
                   COALESCE(
                       est_reclaimable_bytes,
                       CASE
                           WHEN avg_leaf_density IS NULL THEN NULL
                           WHEN avg_leaf_density = 'NaN'::double precision THEN 0::bigint
                           ELSE GREATEST(
                               0,
                               (index_bytes * (90.0 - avg_leaf_density) / NULLIF(90.0, 0))::bigint)
                       END) AS estimated_reclaimable_bytes,
                   /* The discriminator, and it is not cosmetic: a NULL avg_leaf_density means this row was
                      ESTIMATED, not that an exact measurement came back empty, and those two readings lead
                      an operator to opposite conclusions. Keyed on est_tuple_bytes rather than on the
                      density, because the estimate populates it even when the gate suppresses the answer -
                      so it distinguishes the row's PROVENANCE rather than its outcome. */
                   CASE WHEN est_tuple_bytes IS NOT NULL THEN 'estimated' ELSE 'measured' END
                       AS measurement_kind,
                   est_bloat_pct, index_pages, table_rows, fillfactor, est_tuple_bytes, est_leaf_pages,
                   pgstattuple_available,
                   skipped_reason, collection_time
            FROM pg_index_bloat
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY database_name, schema_name, table_name, index_name,
                     (skipped_reason IS NULL) DESC, collection_time DESC
        ) AS latest
        /* Unmeasured first - a skipped index is the likeliest big win and must not be ranked below measured
           ones by a reclaimable figure it does not have. Then by reclaimable bytes, never by density: a
           small index at 40% is worth nothing next to a large one at 70%. */
        ORDER BY (skipped_reason IS NOT NULL) DESC,
                 latest.estimated_reclaimable_bytes DESC NULLS LAST,
                 index_bytes DESC
        LIMIT $4
        """;

    /// <summary>
    /// One group of the coverage census: whether the collector produced a run at all, and — for one
    /// combination of stored reason and provenance — how many indexes and how many index bytes it accounts
    /// for.
    /// </summary>
    /// <param name="EvidenceCollectorRan">
    /// Whether <c>pg_index_bloat</c> recorded a SUCCEEDING run for this server in the evidence window. A run
    /// and not a row count, and that distinction is the whole reason this field exists: a run that stored no
    /// rows is the measurement establishing that the server has no btree index, so collapsing the two makes
    /// "measured, and there is nothing" indistinguishable from "the collector has not run here".
    ///
    /// <para>Repeated on every group because the probe is a single-row relation the groups hang off, which
    /// is what lets ONE statement carry both a scalar and a variable-length breakdown — and lets a server
    /// with no rows at all still report whether its collector ran.</para>
    /// </param>
    /// <param name="SkippedReason">The collector's stored prose, or null for a trusted index.
    /// <c>skipped_reason IS NULL</c> is the ONLY trust predicate on this collector: <c>est_tuple_bytes</c>
    /// is populated on 100% of suppressed rows and <c>est_bloat_pct</c> on 0% of them, so a census keyed on
    /// either <c>est_</c> column would report complete coverage over a population with none.</param>
    /// <param name="IsEstimate">This group's PROVENANCE, not its outcome — whether the row came from the
    /// statistics model or is an older <c>pgstatindex</c> measurement still inside retention. Read only
    /// alongside <paramref name="SkippedReason"/>, never instead of it.</param>
    public readonly record struct PgIndexBloatCoverageGroup(
        bool EvidenceCollectorRan,
        string? SkippedReason,
        bool IsEstimate,
        long IndexCount,
        long IndexBytes);

    /// <summary>
    /// <see cref="EnumeratedCollectorDriver.FreshnessSuccessStatuses"/> as a SQL <c>IN</c> list. Built from
    /// the shared list rather than retyped so the bar for "this collector produced valid evidence" is the
    /// one the freshness reads and the self-alert evaluator already apply, and so a status added there
    /// reaches this probe without anybody remembering to come here.
    ///
    /// <para>Literal-safe by construction: every element is a compile-time constant in this repo's own
    /// source, never operator input.</para>
    /// </summary>
    private static readonly string EvidenceStatusList = string.Join(
        ", ",
        EnumeratedCollectorDriver.FreshnessSuccessStatuses.Select(status => "'" + status + "'"));

    /* THE LATEST ROW PER INDEX, with the SAME tie-break the row read uses - (skipped_reason IS NULL) DESC
       before collection_time DESC. Not a coincidence and not copied for tidiness: this census decides
       whether an index counts as answered, and the row read decides what it displays for that index. If the
       two disagreed about which of an index's rows is current, the census would publish a trusted count the
       grid beside it contradicts, index by index, with nothing that fails.

       DISTINCT ON is what makes it a POPULATION rather than a history. pg_index_bloat writes every btree on
       every cycle, so a plain aggregate over a 48-hour window counts each index once per run and would
       report double the server's index count and double its footprint - a denominator wrong in the
       direction that makes coverage look better than it is.

       database_name leads the key (#2599): this collector runs once per database and an index name is only
       unique within one, so without it a shared schema's copies collapse into whichever collected last.

       THE PROBE IS A SINGLE-ROW RELATION THE GROUPS HANG OFF, via LEFT JOIN ... ON true. That is the whole
       reason one statement can carry a scalar and a variable-length breakdown: a server whose collector ran
       and stored NOTHING still returns one row carrying the probe, where an inner join would drop exactly
       the row that separates "no indexes" from "no evidence". EXISTS rather than count(*) because only the
       zero test is read.

       AND IT COUNTS ONLY RUNS THAT SUCCEEDED, which is not a detail here. Measured on a live Aurora target
       on 2026-09-10 this collector recorded 7 runs in seven days and 3 of them ERRORED - one a 300-second
       client-side command deadline that stored nothing at all. An unfiltered probe on a window landing on
       such a cycle would say "it ran" over zero rows, and the classifier would answer NoCandidates,
       "nothing to fix", on a server whose collector is timing out. That is this issue's own false innocence
       relocated into the collector's health. The status set comes from
       EnumeratedCollectorDriver.FreshnessSuccessStatuses rather than being retyped.

       GROUPING ON THE REASON PROSE, then keying it in C#. The reason is a 250-character paragraph repeated
       on every row - 2,954 identical copies in the fleet's largest bucket - so grouping first reduces it to
       at most a dozen rows, and the classifier maps prose to a short stable key with an Unrecognized arm for
       text this build does not know. Keying in SQL instead would put the marker list in two places.

       $1 server_id, $2 window start, $3 window end. */
    public static readonly string CoverageEvidenceSql = @"
WITH latest AS (
    SELECT DISTINCT ON (database_name, schema_name, table_name, index_name)
           index_bytes, skipped_reason, est_tuple_bytes
    FROM pg_index_bloat
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    ORDER BY database_name, schema_name, table_name, index_name,
             (skipped_reason IS NULL) DESC, collection_time DESC
),
probe AS (
    SELECT EXISTS (
        SELECT 1
        FROM collection_log
        WHERE server_id = $1
        AND   collector_name = 'pg_index_bloat'
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   status IN (" + EvidenceStatusList + @")
    ) AS evidence_collector_ran
),
grouped AS (
    SELECT latest.skipped_reason                              AS skipped_reason,
           (latest.est_tuple_bytes IS NOT NULL)               AS is_estimate,
           count(*)::bigint                                   AS index_count,
           COALESCE(sum(latest.index_bytes), 0)::bigint       AS index_bytes
    FROM latest
    GROUP BY latest.skipped_reason, (latest.est_tuple_bytes IS NOT NULL)
)
SELECT probe.evidence_collector_ran,
       grouped.skipped_reason,
       grouped.is_estimate,
       grouped.index_count,
       grouped.index_bytes
FROM probe
LEFT JOIN grouped ON true";

    /// <summary>
    /// The coverage verdict for a <c>get_pg_index_bloat</c> read — what share of this server's btree
    /// footprint has an answer, by count AND by bytes, and the sentence saying so (#3278).
    ///
    /// <para>A failed evidence read answers <see cref="PgIndexBloatCoverageArm.Undetermined"/> with its own
    /// wording rather than throwing, the same rule <c>DarlingRuntimePrecondition</c> follows: this runs to
    /// EXPLAIN a result the caller already has, and turning that into a read error would replace an
    /// under-described answer with no answer.</para>
    ///
    /// <para><b>No window START, deliberately, and the signature says so by not having one.</b> The evidence
    /// span is <see cref="EvidenceStart"/> to <paramref name="endUtc"/> whatever window the caller read its
    /// rows over — see that method for why. An accepted-but-ignored <c>startUtc</c> is worse than an absent
    /// one: the caller passes a window, reasonably believes it is honoured, and neither the compiler nor the
    /// answer tells them otherwise. Callers wanting the raw groups over a span of their own have
    /// <see cref="GetCoverageEvidenceAsync"/>, which takes both ends BECAUSE it uses both.</para>
    /// </summary>
    /// <param name="endUtc">The instant the caller's read ENDS at, which the evidence window is anchored on
    /// so an <c>as_of</c> read is explained by contemporary evidence rather than by today's.</param>
    /// <param name="returnedRows">How many rows the caller's read returned. Reported as its own labelled
    /// figure and never divided into a population one.</param>
    public static async Task<PgIndexBloatCoverageVerdict> GetCoverageVerdictAsync(
        NpgsqlDataSource postgres, int serverId, DateTime endUtc, int returnedRows,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        try
        {
            var groups = await GetCoverageEvidenceAsync(
                postgres, serverId, EvidenceStart(endUtc), endUtc, cancellationToken);

            var estimated = default(PgIndexBloatTally);
            var exactlyMeasured = default(PgIndexBloatTally);
            var suppressed = new List<PgIndexBloatSuppressionBucket>();

            foreach (var group in groups)
            {
                var tally = new PgIndexBloatTally(group.IndexCount, group.IndexBytes);

                /* skipped_reason, and nothing else. A group's IsEstimate says where its figure CAME FROM
                   and never whether there is one - est_tuple_bytes is populated on every suppressed row in
                   every bucket, so splitting on provenance first would file 6,680 answerless indexes as
                   estimates. */
                if (group.SkippedReason is null)
                {
                    if (group.IsEstimate)
                    {
                        estimated += tally;
                    }
                    else
                    {
                        exactlyMeasured += tally;
                    }

                    continue;
                }

                suppressed.Add(new PgIndexBloatSuppressionBucket(
                    PgIndexBloatCoverage.Bucket(group.SkippedReason),
                    group.IndexCount,
                    group.IndexBytes,
                    group.SkippedReason));
            }

            return PgIndexBloatCoverage.Classify(
                groups.Count > 0 && groups[0].EvidenceCollectorRan,
                estimated,
                exactlyMeasured,
                suppressed,
                returnedRows);
        }
        catch (Exception)
        {
            return PgIndexBloatCoverage.EvidenceUnreadable(returnedRows);
        }
    }

    /// <summary>
    /// The evidence lookback: a FIXED span ending where the caller's read ends, independent of how wide a
    /// window the caller asked its rows over.
    ///
    /// <para><b>The read's own window is the wrong window for this, in both directions.</b> A one-hour
    /// panel window straddles none of a daily collector's runs, so it would report "no evidence" for a
    /// target measured every day for a week. And a wide one is no better than wasteful: the MCP tool
    /// defaults to 168 hours, and this census runs on every call whether the result is empty or not, over a
    /// hypertable where a wider span means more chunks for a figure that describes the present.</para>
    ///
    /// <para><b>Because it is not a question about history.</b> What share of this server's indexes are
    /// modellable is a CURRENT state, and <c>CollectorRuntimePrecondition</c> settled this shape already: it
    /// consults the LATEST run rather than a window, on the reasoning that a precondition somebody has since
    /// satisfied must not keep being reported. Averaging a week of candidate counts would answer a question
    /// nobody asked and cost more to do it.</para>
    ///
    /// <para>Anchored on <paramref name="endUtc"/>, so an <c>as_of</c> read gets the evidence contemporary
    /// with the data it is explaining rather than today's.</para>
    ///
    /// <para>The span itself lives on <see cref="PgIndexBloatCoverage.EvidenceHours"/>, not here: it appears
    /// in the census an operator reads, so the label and the query have to be the same figure and a copy in
    /// this file is how they stop being. That constant also carries WHY it is two of the collector's
    /// cadences rather than one.</para>
    /// </summary>
    internal static DateTime EvidenceStart(DateTime endUtc) =>
        endUtc.AddHours(-PgIndexBloatCoverage.EvidenceHours);

    /// <summary>The raw census groups, for callers that want the figures rather than the sentence.</summary>
    public static async Task<List<PgIndexBloatCoverageGroup>> GetCoverageEvidenceAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var groups = new List<PgIndexBloatCoverageGroup>();
        await using var command = postgres.CreateCommand(CoverageEvidenceSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the bind, for the reason the row read below documents: Kind=Utc infers
           timestamptz and the comparison against these naive columns then resolves at the store session's
           TimeZone, which east of UTC slides the window off the data. An evidence read that silently
           returned nothing would answer Undetermined on a server that has the evidence. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            groups.Add(new PgIndexBloatCoverageGroup(
                EvidenceCollectorRan: !reader.IsDBNull(0) && reader.GetBoolean(0),
                SkippedReason: reader.IsDBNull(1) ? null : reader.GetString(1),
                /* NULL on the probe-only row a server with no stored rows produces, which is neither
                   provenance - so it must not default to the estimate arm and be counted as an answer. The
                   count and bytes are null on that row too, so it contributes nothing either way. */
                IsEstimate: !reader.IsDBNull(2) && reader.GetBoolean(2),
                IndexCount: reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                IndexBytes: reader.IsDBNull(4) ? 0 : reader.GetInt64(4)));
        }

        return groups;
    }

    public static async Task<List<PgIndexBloatRow>> GetPgIndexBloatAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgIndexBloatRow>();
        await using var command = postgres.CreateCommand(PgIndexBloatSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the BIND, same convention as every other PostgreSQL read here: Npgsql
           does not reject Kind=Utc, it infers timestamptz, and PostgreSQL then resolves the comparison
           against these NAIVE timestamp columns at the store session's TimeZone — so east of UTC the window
           slides off the data and the read returns nothing at all. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgIndexBloatRow(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                SchemaName: reader.IsDBNull(1) ? null : reader.GetString(1),
                TableName: reader.IsDBNull(2) ? null : reader.GetString(2),
                IndexName: reader.IsDBNull(3) ? null : reader.GetString(3),
                IndexBytes: reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                TreeLevel: reader.IsDBNull(5) ? null : reader.GetInt32(5),
                EmptyPages: reader.IsDBNull(6) ? null : reader.GetInt64(6),
                DeletedPages: reader.IsDBNull(7) ? null : reader.GetInt64(7),
                AvgLeafDensity: reader.IsDBNull(8) ? null : reader.GetDouble(8),
                LeafFragmentation: reader.IsDBNull(9) ? null : reader.GetDouble(9),
                EstimatedReclaimableBytes: reader.IsDBNull(10) ? null : reader.GetInt64(10),
                SkippedReason: reader.IsDBNull(11) ? null : reader.GetString(11),
                MeasurementKind: reader.IsDBNull(12) ? null : reader.GetString(12),
                EstBloatPct: reader.IsDBNull(13) ? null : reader.GetDouble(13),
                IndexPages: reader.IsDBNull(14) ? null : reader.GetInt64(14),
                TableRows: reader.IsDBNull(15) ? null : reader.GetInt64(15),
                Fillfactor: reader.IsDBNull(16) ? null : reader.GetInt32(16),
                EstTupleBytes: reader.IsDBNull(17) ? null : reader.GetInt64(17),
                EstLeafPages: reader.IsDBNull(18) ? null : reader.GetInt64(18),
                PgstattupleAvailable: reader.IsDBNull(19) ? null : reader.GetBoolean(19),
                CaptureTime: reader.IsDBNull(20)
                    ? default
                    : DateTime.SpecifyKind(reader.GetDateTime(20), DateTimeKind.Utc)));
        }

        return rows;
    }
}
