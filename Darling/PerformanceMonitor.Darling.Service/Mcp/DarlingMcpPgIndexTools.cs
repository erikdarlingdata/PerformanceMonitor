/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for ESTIMATED index bloat and for column distribution statistics — the two PostgreSQL
/// reads that answer "why is this plan shaped like that", paired with the <c>pg_index_bloat</c> and
/// <c>pg_column_stats</c> collectors (#2629).
///
/// <para>
/// <c>get_pg_index_bloat</c> and <c>get_pg_table_bloat</c> are now the same KIND of read: both estimate
/// from catalog statistics and both suppress the figure when the statistics cannot support one. #3234
/// changed index bloat from a <c>pgstatindex</c> census to that shape, because walking every page of every
/// index is the analogue of <c>DETAILED</c> index physical stats on SQL Server and nobody schedules
/// <c>DETAILED</c> against production. A row carrying <c>skipped_reason</c> therefore has NO answer rather
/// than a healthy one, and the reason separates the remediable — a never-analyzed parent, invisible column
/// widths — from the structural, being a PARTIAL or DEDUPLICATED index that no statistics model can reach
/// at any grant or freshness. Those are what <c>exact_measurement_command</c> is for, and it is the
/// <c>pgstatindex</c> call this tool used to make on a schedule.
/// </para>
///
/// <para><b>Row counts differ from <c>get_pg_index_usage</c> by design</b> (#3158):
/// <c>pg_index_bloat</c> is the COMPLETE btree census with no size floor, while
/// <c>pg_index_usage_stats</c> reports only indexes of at least 64 kB (plus any invalid index at any
/// size). Measured on one target: 2,500 against 1,517, a 65% difference that is entirely that one floor.
/// Neither is missing objects.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgIndexTools
{
    [McpServerTool(Name = "get_pg_index_bloat"), Description("Gets ESTIMATED PostgreSQL btree index bloat from catalog stats (no page reads), ranked by reclaimable BYTES, never percentage. measurement_kind: estimated (stats model) or measured (an older pgstatindex reading in retention). skipped_reason present = NO answer, not healthy; remediable (ANALYZE, a grant) vs structurally unmodellable (PARTIAL/DEDUPLICATED) - use exact_measurement_command instead. Answerless rows sort FIRST by design: read coverage before judging reach, and pass answered_only=true to reach real answers - est_tuple_bytes alone looks complete even on skipped rows. <<GUIDE>> Gets ESTIMATED PostgreSQL btree index bloat, computed from catalog statistics with NO page reads: how many bytes a REINDEX could plausibly reclaim, the modelled tuple width and leaf-page count it rests on, and the parent row count and fillfactor those came from. Ranked by reclaimable BYTES and never by percentage - a 64 kB index at 20% tops a percentage-ranked list and is worth 50 kB next to a 10 GB index at 45% worth 5.37 GB. Read measurement_kind: 'estimated' rows come from the statistics model, 'measured' rows are older pgstatindex measurements still inside the retention window. Accuracy against pgstatindex ground truth on a live 2,500-index target: median absolute error 2.79 percentage points, p90 6.63. A row with a skipped_reason has NO answer rather than a healthy one - never read a missing estimate as a clean bill of health - and the reason says whether it is remediable: a never-analyzed parent needs an ANALYZE, invisible column widths need the pg_read_all_data grant, while a PARTIAL index and a DEDUPLICATED one (low-cardinality, non-unique) are structurally unmodellable at any grant or statistics freshness and need the exact function instead. Every row carries exact_measurement_command, which is the pgstatindex call for that index: it walks every page, so run it deliberately on the one index you are about to act on rather than on a schedule - the same relationship SQL Server has between LIMITED and DETAILED index physical stats. This is the COMPLETE btree census with no size floor, so it returns MORE rows than get_pg_index_usage, which floors at 64 kB - 2,500 against 1,517 on one measured target. Neither census is missing objects. ALWAYS read the coverage field before concluding anything about how much of a server this covers, and never infer that from the rows: answerless rows sort FIRST by design, so any limit smaller than the answerless population returns 100% skipped rows structurally rather than by chance, and raising the limit does not fix it while that population is still larger. coverage comes from a separate population-level query that no limit touches - candidate indexes, trusted versus suppressed, and each suppression reason - and every count carries its BYTES because the two rankings disagree: on a live fleet the reason that is second largest by row count is the SMALLEST by footprint at 0.0004% of it, while the fourth largest by rows is second by bytes at 1.4 TB. Judge the gap by bytes. And skipped_reason IS NULL is the ONLY trust predicate here: est_tuple_bytes is populated on 100% of the rows that have no answer and est_bloat_pct on 0% of them, so filtering on est_tuple_bytes returns every row and reads as complete coverage. To READ the answers, pass answered_only: true - the answerless-first sort means that on a big server the answers sort behind more answerless rows than any permitted limit can page past, measured at 1,779 answerless ahead of 726 answers covering 213.8 GB against a 1000-row maximum, so raising the limit cannot reach them and answered_only is the only route. The reach field says which situation you are in and whether a larger limit would help: Unreachable means it would not, at any value, ever. answered_only leaves the default order alone and the coverage census unchanged, so a filtered ranking still carries its denominator - and answered_only over a server where NOTHING has an answer returns status no_answers rather than an empty, because \"nothing measurable\" is not \"nothing to reclaim\". This is what bounds the page - read truncated to know whether the census held more indexes than were returned; it is observed by fetching one row past this cap, never inferred from a full page, and the coverage field is measured independently of it. answered_only defaults to false, which leaves the answerless-first order alone. Set answered_only when you want the RANKING: answerless rows sort first by design, so on a server whose answerless population exceeds the 1000-row maximum the answers cannot be reached at any limit - measured at 1,779 answerless ahead of 726 answers covering 213.8 GB. The coverage census is unaffected by this, so a filtered ranking still carries the same denominator; read the reach field to see which situation you are in.")]
    public static async Task<string> GetPgIndexBloat(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 168 (7 days) - this collector runs daily.")] int hours_back = 168,
        [Description("Maximum rows to return. Default 25. See the tool's reading guide.")] int limit = 25,
        [Description("Return ONLY the indexes that have an answer, ranked by reclaimable bytes descending. Default false. See the tool's reading guide.")] bool answered_only = false,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            /* #3653 (the #3541 A3 class, the residue #3679 named): limit + 1 as the fetch, the extra row as the
               OBSERVED truncation signal, and the page trimmed back to `limit` HERE - before the empty branch,
               the coverage verdict and the reach classifier - so every `rows.Count` below is a count of the
               page and never of the over-fetch. The old `rows.Count >= limit` said "more" for a server whose
               candidate census was exactly `limit` indexes long and, because this tool withholds its three
               per-page counts when truncated, withheld answered_count / estimated_count /
               exactly_measured_count for a page that was the whole census. PgCappedRead.Classify still
               receives the page count and the cap, as it did: its `ReturnedRows < Limit -> Complete` test is
               documented as deliberately pessimistic and is not this change's to soften. */
            var fetched = await DarlingPgIndexBloatReader.GetPgIndexBloatAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit + 1,
                answered_only, cancellationToken);
            var (rows, truncated) = McpHelpers.BoundPage(fetched, limit);

            /* Asked on BOTH paths, not just the empty one (#3278). A returned page that is 100% suppressed
               rows is the same defect as an unexplained empty and strictly harder to see: the read looks
               like it worked, and the unmeasured-first sort GUARANTEES that shape whenever the suppressed
               population exceeds the limit. */
            PgIndexBloatCoverageVerdict coverage;

            if (rows.Count == 0)
            {
                /* CAPABILITY, then PRECONDITION, then this read's own miss - the order
                   CollectorRuntimePrecondition documents, asked in it rather than composed with ?? over
                   three already-computed values. Computing the LAST of three ranked answers FIRST is how
                   somebody later reorders the chain and does not notice they have changed which one wins. */
                var capability = await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_index_bloat", cancellationToken);

                if (capability != null) return capability;

                var precondition = await DarlingRuntimePrecondition.StatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_index_bloat", cancellationToken);

                if (precondition != null) return precondition;

                coverage = await DarlingPgIndexBloatReader.GetCoverageVerdictAsync(
                    postgres, resolved.ServerId, windowEnd, rows.Count, cancellationToken);

                /* ANSWERED-ONLY OVER A SERVER THAT HAS INDEXES IS ITS OWN REFUSAL, not an empty (#3424).
                   The filter removes the answerless rows, so "no rows" here means NOT ONE of this server's
                   indexes has an answer - which is the NothingTrusted state, and the "widen the window"
                   sentence below would be advice for a different situation entirely. Worse, an empty
                   status over a filtered read is the exact failure both these issues are about: it reads
                   as "nothing to reclaim" when the truth is "nothing measurable". Its own token, for the
                   reason EXTENSION_MISSING has one (#3240) - a refusal with its own cause and its own
                   remedy bands apart rather than folding into the nearest existing label. */
                if (answered_only && coverage.Candidates.IndexCount > 0)
                {
                    return McpHelpers.Status(
                        "no_answers",
                        $"answered_only excluded every row: {resolved.ServerName} has "
                        + $"{coverage.Candidates.IndexCount:N0} candidate btree index(es) in this window and "
                        + "NOT ONE of them has a trusted bloat answer. This is not a clean bill of health "
                        + "and it is not an empty server - the suppression breakdown below says why each "
                        + "index has no answer and which reasons are remediable. Re-run without "
                        + "answered_only to see the rows and their reasons. " + coverage.Message,
                        new
                        {
                            arm = coverage.Arm.ToString(),
                            candidate_index_count = coverage.Candidates.IndexCount,
                            candidate_index_bytes = coverage.Candidates.IndexBytes,
                            trusted_index_count = coverage.Trusted.IndexCount,
                            trusted_index_bytes = coverage.Trusted.IndexBytes,
                            suppressed_by_reason = coverage.Suppressed.Select(bucket => new
                            {
                                reason = bucket.Reason.ToString(),
                                label = PgIndexBloatCoverage.Label(bucket.Reason),
                                index_count = bucket.IndexCount,
                                index_bytes = bucket.IndexBytes,
                                remedy = PgIndexBloatCoverage.Remedy(bucket.Reason),
                            }),
                        });
                }

                return McpHelpers.Status(
                    "empty",
                    $"No index bloat rows for {resolved.ServerName} in the last {hours_back} "
                    + "hour(s). This collector runs DAILY, so a window shorter than a day can be empty "
                    + "on a perfectly healthy server — widen it before concluding anything. "
                    + coverage.Message);
            }

            coverage = await DarlingPgIndexBloatReader.GetCoverageVerdictAsync(
                postgres, resolved.ServerId, windowEnd, rows.Count, cancellationToken);

            /* WHETHER THE ANSWERS ARE REACHABLE AT ALL, which `truncated` cannot say (#3424). #3278 gave
               this read a denominator and that shipped; it did not give the answers a route, and its own
               opening sentence says why raising the limit is not one. The population figures are already in
               hand here, so the arithmetic is: the answerless rows sort FIRST, so they are what stands
               between the caller's limit and the trusted rows - and once that leading block is at least
               McpHelpers.MaxTop, no permitted limit reaches a single answer.

               Under answered_only the leading block is GONE, so nothing is ahead of the answered population
               and a capped page is the top N of it by reclaimable bytes - the RankedTail arm, which is the
               ranking this tool exists to produce rather than a gap in it.

               MaxTop is passed as the symbol rather than 1000: this classifier decides the difference
               between "raise the limit" and "raising the limit cannot help", and deciding it against a
               literal that has drifted from what ValidateTop enforces would answer confidently for a
               surface that does not exist.

               RANKED, on both paths (#3435). The outer ORDER BY's remaining keys are estimated reclaimable
               bytes descending then index bytes descending, so over the ANSWERED population - the one this
               classifier is asked about - the order is a ranking whether or not answered_only removed the
               answerless block in front of it. That declaration is what lets this surface reach RankedTail
               at all, and the arm's claim that the withheld rows rank lower is exactly this ORDER BY's
               property rather than a sentence bolted onto the arm. */
            var reach = PgCappedRead.Classify(
                rowsAhead: answered_only ? 0 : coverage.Suppressed.Sum(bucket => bucket.IndexCount),
                wantedRows: coverage.Trusted.IndexCount,
                returnedRows: rows.Count,
                limit: limit,
                maxLimit: McpHelpers.MaxTop,
                order: PgOrderSemantics.Ranked,
                remedy: "Pass answered_only: true to request the answered indexes directly, ranked by "
                      + "reclaimable bytes descending. That leaves the default answerless-first order "
                      + "untouched for readers who have not asked, and the coverage census below still "
                      + "reports what the filter excluded.");

            var answered = rows.Count(r => r.SkippedReason is null);
            var estimated = rows.Count(r => r.SkippedReason is null && r.IsEstimate);
            var measured = rows.Count(r => r.SkippedReason is null && !r.IsEstimate);
            /* An EMPTY index is the one row whose null density does NOT mean "not measured", and nothing
               else in the payload distinguishes it: pgstatindex has no leaf pages to derive a density from,
               so the read nulls it, while skipped_reason stays null because the measurement succeeded. Named
               here because the note below spends two sentences teaching the reader that a null measurement
               is absence of data, and on this row that lesson is wrong. */
            var empty = rows.Count(r =>
                r.SkippedReason is null && !r.IsEstimate && r.AvgLeafDensity is null);

            var indexes = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                schema_name = r.SchemaName,
                table_name = r.TableName,
                index_name = r.IndexName,
                index_bytes = r.IndexBytes,
                index_mb = Math.Round(r.IndexBytes / 1024.0 / 1024.0, 1),
                /* Null on a skipped row, and deliberately not zero: zero density would read as a
                   catastrophically bloated index, which is the opposite of "we did not look". Also null on
                   an empty index, where there are no leaf pages to have a density - the note distinguishes
                   the two, since only one of them means the index was not looked at. */
                avg_leaf_density = r.AvgLeafDensity,
                leaf_fragmentation = r.LeafFragmentation,
                tree_level = r.TreeLevel,
                empty_pages = r.EmptyPages,
                deleted_pages = r.DeletedPages,
                estimated_reclaimable_bytes = r.EstimatedReclaimableBytes,
                estimated_reclaimable_mb = r.EstimatedReclaimableBytes is { } bytes
                    ? Math.Round(bytes / 1024.0 / 1024.0, 1)
                    : (double?)null,
                /* Present means NOT MEASURED. Named rather than boolean so the row says WHY. */
                skipped_reason = r.SkippedReason,
                /* WHEN the measurement was taken, which under rotation is not "the last cycle" (#3153): the
                   collector measures a slice per cycle, so this row can be most of a pass old. Null on a
                   labelled row rather than that row's own timestamp, which would read as the age of a
                   measurement that does not exist - see PgIndexBloatRow.MeasuredAt, which the grid binds
                   to as well so the two surfaces cannot disagree about it. */
                measured_at = r.MeasuredAt?.ToString("o"),

                /* PROVENANCE, not outcome. A null avg_leaf_density on an 'estimated' row means this index
                   was never walked; on a 'measured' row it means the walk found no leaf pages. Those two
                   readings lead an operator to opposite conclusions, so the row says which it is instead
                   of leaving it to be inferred from a null. */
                measurement_kind = r.MeasurementKind,
                estimated_at = r.EstimatedAt?.ToString("o"),

                /* The estimate and every input it rests on, so the number can be argued with rather than
                   believed - and so a later change to the width model can be checked against history. */
                est_bloat_pct = r.EstBloatPct,
                index_pages = r.IndexPages,
                table_rows = r.TableRows,
                fillfactor = r.Fillfactor,
                est_tuple_bytes = r.EstTupleBytes,
                est_leaf_pages = r.EstLeafPages,

                pgstattuple_available = r.PgstattupleAvailable,
                /* From the row, not rebuilt here: the grid binds the same property, and a second copy of
                   this string is a second thing to get wrong. */
                exact_measurement_command = r.ExactMeasurementCommand,
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                index_count = rows.Count,
                truncated,
                /* ECHOED, because a saved payload has to say which population it describes. A ranking of
                   726 answered indexes and a ranking of 2,505 candidates are different answers and nothing
                   else in the response distinguishes them. */
                answered_only,
                /* WHETHER THE ANSWERS CAN BE REACHED, beside `truncated` rather than instead of it: one
                   says the page stopped, the other says whether what it stopped short of is obtainable at
                   all. Named arms rather than a boolean because "raise the limit" and "raising the limit
                   cannot help" are different instructions. */
                reach = new
                {
                    arm = reach.Reach.ToString(),
                    /* WHAT THE ORDER MEANS, beside the arm rather than implied by it (#3435). RankedTail is
                       reachable only from a ranking, so this field is what a reader checks to know the arm
                       could have been that - and on a grouped read it is what says the missing rows are
                       other groups rather than a lower-ranked remainder. */
                    order_semantics = reach.Order.ToString(),
                    is_complete = reach.IsComplete,
                    a_raised_limit_would_help = reach.ARaisedLimitWouldHelp,
                    answerless_rows_ahead = reach.RowsAhead,
                    answered_rows_on_server = reach.WantedRows,
                    answered_rows_reachable_here = reach.ReachableRows,
                    answered_rows_reachable_at_max_limit = reach.ReachableAtMaxRows,
                    answered_rows_withheld = reach.WithheldRows,
                    limit,
                    max_limit = McpHelpers.MaxTop,
                },
                /* Over the returned rows, and withheld when they are only a page of them: "12 of 25
                   measured" reads as a statement about the server's indexes and would not be one. */
                /* Over the RETURNED rows, and withheld when they are only a page of them: "12 of 25
                   estimated" reads as a statement about the server's indexes and would not be one. Split
                   three ways because the three mean different things - an answer from the model, an older
                   exact measurement still inside retention, and no answer at all. */
                answered_count = truncated ? (int?)null : answered,
                estimated_count = truncated ? (int?)null : estimated,
                exactly_measured_count = truncated ? (int?)null : measured,

                /* THE POPULATION FIGURES, which the three above are not and were never able to be (#3278).
                   These come from a separate query over every candidate index on the server, so they are
                   unaffected by the limit and by the unmeasured-first sort - which is what lets a page of
                   all-skipped rows stop reading as a coverage claim. Every count carries its BYTES, because
                   on the live fleet the two rank the suppression buckets differently: the bucket that is
                   second of five by rows is LAST by footprint at 0.0004% of it. */
                coverage = new
                {
                    /* The arm as its own field, beside the sentence. Automation keys on names rather than
                       parsing prose, and a caller deciding whether this ranking is safe to act on needs the
                       partial-coverage answer in a form it can branch on. */
                    arm = coverage.Arm.ToString(),
                    evidence_hours = PgIndexBloatCoverage.EvidenceHours,
                    candidate_index_count = coverage.Candidates.IndexCount,
                    candidate_index_bytes = coverage.Candidates.IndexBytes,
                    trusted_index_count = coverage.Trusted.IndexCount,
                    trusted_index_bytes = coverage.Trusted.IndexBytes,
                    estimated_index_count = coverage.Estimated.IndexCount,
                    estimated_index_bytes = coverage.Estimated.IndexBytes,
                    exactly_measured_index_count = coverage.ExactlyMeasured.IndexCount,
                    exactly_measured_index_bytes = coverage.ExactlyMeasured.IndexBytes,
                    /* Ranked by BYTES by the classifier, not here - one ordering, so this payload and the
                       WPF note cannot present two different "largest cause". */
                    suppressed_by_reason = coverage.Suppressed.Select(bucket => new
                    {
                        reason = bucket.Reason.ToString(),
                        label = PgIndexBloatCoverage.Label(bucket.Reason),
                        index_count = bucket.IndexCount,
                        index_bytes = bucket.IndexBytes,
                        remedy = PgIndexBloatCoverage.Remedy(bucket.Reason),
                        /* The collector's stored explanation ONCE per bucket. It is a 250-character
                           paragraph repeated on every row - 2,954 identical copies in the fleet's largest
                           bucket - so per-row is where it does not belong. */
                        detail = bucket.Detail,
                    }),
                },
                note = "These are ESTIMATES from catalog statistics, not measurements: no index page is "
                     + "read. Measured against pgstatindex ground truth on a live 2,500-index target, "
                     + "median absolute error is 2.79 percentage points and p90 is 6.63, which is close "
                     + "enough to decide WHICH index to act on and not close enough to justify a REINDEX "
                     + "on its own — use exact_measurement_command for that, on the one index concerned. "
                     + "Rank on estimated_reclaimable_bytes and never on a percentage: a small index at a "
                     + "terrible density is worth kilobytes. Rows carrying a skipped_reason have NO "
                     + "answer, never a clean one, and the reason separates the remediable from the "
                     + "structural: a never-analyzed parent needs an ANALYZE and invisible column widths "
                     + "need the pg_read_all_data grant, while PARTIAL and DEDUPLICATED indexes cannot be "
                     + "modelled at any grant or statistics freshness — PostgreSQL 13+ stores duplicate "
                     + "keys once in a posting list, so real storage is denser than per-tuple arithmetic "
                     + "can predict and a correct model still over-predicts. Those are exactly what the "
                     + "exact function is for. This census has no size floor, so it counts MORE indexes "
                     + "than get_pg_index_usage, which floors at 64 kB — the difference is that floor and "
                     + "nothing else."
                     + (measured > 0
                         ? $" {measured} of the {rows.Count} row(s) returned are older pgstatindex "
                           + "MEASUREMENTS rather than estimates, still inside the retention window; read "
                           + "measurement_kind per row, and measured_at for their age."
                         : string.Empty)
                     + (rows.Count - answered > 0
                         ? $" {rows.Count - answered} of the {rows.Count} row(s) returned carry a reason "
                           + "instead of an answer."
                         : string.Empty)
                     + (empty > 0
                         ? $" {empty} row(s) have NO skipped_reason and a null avg_leaf_density: those "
                           + "indexes are EMPTY. pgstatindex has no leaf pages to derive a density from, so "
                           + "there is no density to report — the measurement succeeded and found nothing, "
                           + "which is why estimated_reclaimable_bytes is 0 rather than null."
                         : string.Empty)
                     + (truncated
                         ? " TRUNCATED at the row limit: there are more indexes than this, and because rows "
                           + "with no answer sort FIRST, a page smaller than the answerless population is "
                           + "100% of them by construction rather than by chance - raising the limit does "
                           + "not fix that while that population is still larger. Do not infer coverage "
                           + "from this page; the coverage field below is measured over every candidate "
                           + "index on the server and needs no limit raised."
                         : string.Empty)
                     /* The census on the POPULATED path too, and last so it is the sentence a reader
                        finishes on. A page that ranks four answered indexes while six thousand are
                        withheld reads as a ranking of the server, and nothing in the rows themselves shows
                        the difference. */
                     + " " + coverage.Message
                     /* THE REACH SENTENCE LAST, after the census, because it is the one that tells a reader
                        whether to make a different call. #3278's census says the answers exist; this says
                        whether this response could ever have contained them. */
                     + " " + reach.Message,
                indexes,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_pg_index_bloat", ex);
        }
    }

    [McpServerTool(Name = "get_pg_column_stats"), Description("Gets PostgreSQL per-column planner statistics from pg_stats: n_distinct, null fraction, average width, correlation, and top-value frequency. One row per column: the LATEST capture inside the window, not a history; this collector runs DAILY, so a window under a day can be empty on a healthy server. n_distinct is a RATIO of table rows when negative (-1 = every value unique), an absolute count when positive - check the sign. Gated: only columns on tables above a size floor, visible to the monitoring login, are collected; always read the coverage field first. <<GUIDE>> Gets PostgreSQL per-column distribution statistics from pg_stats: n_distinct, null fraction, average width, physical correlation, and the frequency of the single most common value. These are the numbers the PLANNER uses, so they explain plan shapes that otherwise look arbitrary. n_distinct is negative when PostgreSQL expresses it as a RATIO of table rows (-1 means every value is unique) and positive when it is an absolute count - do not compare the two without checking the sign. correlation near 1 or -1 means the column's physical order matches its logical order, which is what makes an index range scan cheap; near 0 makes the same scan expensive. A high top_value_frequency is the classic cause of a plan that is right for the common value and wrong for every other one. Only columns on tables above a size floor are collected, and only where the monitoring login can see the statistics - so ALWAYS read the coverage field before acting on this: it names which of those produced the result, and PartialVisibility or StatisticsNotVisible means the statistics you are looking at are not all of them.")]
    public static async Task<string> GetPgColumnStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 168 (7 days) - this collector runs daily.")] int hours_back = 168,
        [Description("Maximum rows to return. Default 25.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var windowStart = windowEnd.AddHours(-hours_back);

            var rows = await DarlingPgColumnStatsReader.GetPgColumnStatsAsync(
                postgres, resolved.ServerId, windowStart, windowEnd, limit, cancellationToken);

            /* Asked on BOTH paths, not just the empty one (#3154). A returned row set that covers a
               fraction of the tables above the floor is the same defect as an unexplained empty, one
               degree weaker: the ranking looks complete and is not. */
            PgColumnStatsCoverageVerdict coverage;

            if (rows.Count == 0)
            {
                /* CAPABILITY, then PRECONDITION, then this read's own miss - the order
                   CollectorRuntimePrecondition documents, and the three are asked in it rather than
                   composed with ?? over three already-computed values. The coverage query used to run
                   ahead of both, so a server that cannot have this surface at all, or whose collector
                   recorded a denial, paid for evidence the ?? chain then threw away - and those are the
                   callers least able to afford a round trip. Worse than the cost: computing the LAST
                   answer first is how somebody later reorders the chain and does not notice they have
                   changed which of the three wins. */
                var capability = await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_column_stats", cancellationToken);

                if (capability != null) return capability;

                var precondition = await DarlingRuntimePrecondition.StatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_column_stats", cancellationToken);

                if (precondition != null) return precondition;

                coverage = await DarlingPgColumnStatsReader.GetCoverageVerdictAsync(
                    postgres, resolved.ServerId, windowEnd, rows.Count, cancellationToken);

                /* The arm, not a list of the arms. This message used to recite the size floor AND the
                   privilege filter and select neither, which is prose about the mechanism rather than a
                   diagnosis of it - measured on a 50-target fleet where the answer was the privilege
                   filter on every one of them and no read said so. */
                return McpHelpers.Status(
                    "empty",
                    $"No column statistics for {resolved.ServerName} in the last {hours_back} hour(s). "
                    + "This collector runs DAILY, so a window shorter than a day can be empty on a "
                    + "perfectly healthy server - widen it before concluding anything. " + coverage.Message);
            }

            coverage = await DarlingPgColumnStatsReader.GetCoverageVerdictAsync(
                postgres, resolved.ServerId, windowEnd, rows.Count, cancellationToken);

            var columns = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                schema_name = r.SchemaName,
                table_name = r.TableName,
                column_name = r.ColumnName,
                /* Passed through with its sign intact. Normalising it to an absolute count would need the
                   row count at the time the sample was taken, which is not stored and would be a guess. */
                n_distinct = r.NDistinct,
                null_frac = r.NullFrac,
                avg_width = r.AvgWidth,
                correlation = r.Correlation,
                top_value_frequency = r.TopValueFrequency,
                /* Null means NO most-common-value list at all, which is itself informative: a perfectly
                   uniform column has none. Zero would claim the list exists and is empty. */
                common_value_count = r.CommonValueCount,
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                column_count = rows.Count,
                /* The arm as its own field, beside the sentence. Automation keys on names rather than
                   parsing prose, and a caller deciding whether this ranking is safe to act on needs the
                   partial-coverage answer in a form it can branch on. */
                coverage = coverage.Arm.ToString(),
                note = "n_distinct is a RATIO of table rows when negative and an absolute count when "
                     + "positive — check the sign before comparing two columns. correlation near ±1 is "
                     + "what makes an index range scan cheap. A null common_value_count means the column "
                     + "has no most-common-value list at all, not that the list is empty. "
                     + coverage.Message,
                columns,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_pg_column_stats", ex);
        }
    }
}
