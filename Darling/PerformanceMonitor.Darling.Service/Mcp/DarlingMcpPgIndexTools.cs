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
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for MEASURED index bloat and for column distribution statistics — the two PostgreSQL
/// reads that answer "why is this plan shaped like that", paired with the <c>pg_index_bloat</c> and
/// <c>pg_column_stats</c> collectors (#2629).
///
/// <para>
/// <c>get_pg_index_bloat</c> is the measured counterpart of <c>get_pg_table_bloat</c>, and the difference
/// is the whole point: table bloat is ESTIMATED from statistics and is suppressed when those statistics
/// cannot be trusted, while this reads <c>pgstatindex</c>, which walks the index. That costs real I/O, so
/// the collector measures a bounded slice per cycle, ROTATES that slice down the size order, and LABELS
/// the rest rather than dropping them — a row carrying <c>skipped_reason</c> is a real index that was not
/// measured, not a healthy one, and the reason distinguishes an index that is DEFERRED to a later cycle
/// from one that is over the measurement ceiling and never measured at all (#3153).
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
    [McpServerTool(Name = "get_pg_index_bloat"), Description("Gets ESTIMATED PostgreSQL btree index bloat, computed from catalog statistics with NO page reads: how many bytes a REINDEX could plausibly reclaim, the modelled tuple width and leaf-page count it rests on, and the parent row count and fillfactor those came from. Ranked by reclaimable BYTES and never by percentage - a 64 kB index at 20% tops a percentage-ranked list and is worth 50 kB next to a 10 GB index at 45% worth 5.37 GB. Read measurement_kind: 'estimated' rows come from the statistics model, 'measured' rows are older pgstatindex measurements still inside the retention window. Accuracy against pgstatindex ground truth on a live 2,500-index target: median absolute error 2.79 percentage points, p90 6.63. A row with a skipped_reason has NO answer rather than a healthy one - never read a missing estimate as a clean bill of health - and the reason says whether it is remediable: a never-analyzed parent needs an ANALYZE, invisible column widths need the pg_read_all_data grant, while a PARTIAL index and a DEDUPLICATED one (low-cardinality, non-unique) are structurally unmodellable at any grant or statistics freshness and need the exact function instead. Every row carries exact_measurement_command, which is the pgstatindex call for that index: it walks every page, so run it deliberately on the one index you are about to act on rather than on a schedule - the same relationship SQL Server has between LIMITED and DETAILED index physical stats. This is the COMPLETE btree census with no size floor, so it returns MORE rows than get_pg_index_usage, which floors at 64 kB - 2,500 against 1,517 on one measured target. Neither census is missing objects.")]
    public static async Task<string> GetPgIndexBloat(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 168 (7 days) - this collector runs daily.")] int hours_back = 168,
        [Description("Maximum rows to return. Default 25.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var rows = await DarlingPgIndexBloatReader.GetPgIndexBloatAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit);

            if (rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_index_bloat")
                    ?? await DarlingRuntimePrecondition.StatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_index_bloat")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No index bloat measurements for {resolved.ServerName} in the last {hours_back} "
                        + "hour(s). This collector runs DAILY, so a window shorter than a day can be empty "
                        + "on a perfectly healthy server — widen it before concluding anything.");
            }

            var truncated = rows.Count >= limit;
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
                /* Over the returned rows, and withheld when they are only a page of them: "12 of 25
                   measured" reads as a statement about the server's indexes and would not be one. */
                /* Over the RETURNED rows, and withheld when they are only a page of them: "12 of 25
                   estimated" reads as a statement about the server's indexes and would not be one. Split
                   three ways because the three mean different things - an answer from the model, an older
                   exact measurement still inside retention, and no answer at all. */
                answered_count = truncated ? (int?)null : answered,
                estimated_count = truncated ? (int?)null : estimated,
                exactly_measured_count = truncated ? (int?)null : measured,
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
                         ? " TRUNCATED at the row limit: there are more indexes than this. Raise the limit "
                           + "before concluding anything about the server as a whole."
                         : string.Empty),
                indexes,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL index bloat failed: {ex.Message}");
        }
    }

    [McpServerTool(Name = "get_pg_column_stats"), Description("Gets PostgreSQL per-column distribution statistics from pg_stats: n_distinct, null fraction, average width, physical correlation, and the frequency of the single most common value. These are the numbers the PLANNER uses, so they explain plan shapes that otherwise look arbitrary. n_distinct is negative when PostgreSQL expresses it as a RATIO of table rows (-1 means every value is unique) and positive when it is an absolute count - do not compare the two without checking the sign. correlation near 1 or -1 means the column's physical order matches its logical order, which is what makes an index range scan cheap; near 0 makes the same scan expensive. A high top_value_frequency is the classic cause of a plan that is right for the common value and wrong for every other one. Only columns on tables above a size floor are collected, and only where the monitoring login can see the statistics - so ALWAYS read the coverage field before acting on this: it names which of those produced the result, and PartialVisibility or StatisticsNotVisible means the statistics you are looking at are not all of them.")]
    public static async Task<string> GetPgColumnStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 168 (7 days) - this collector runs daily.")] int hours_back = 168,
        [Description("Maximum rows to return. Default 25.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var windowStart = windowEnd.AddHours(-hours_back);

            var rows = await DarlingPgColumnStatsReader.GetPgColumnStatsAsync(
                postgres, resolved.ServerId, windowStart, windowEnd, limit);

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
                    postgres, resolved.ServerId, resolved.ServerName, "pg_column_stats");

                if (capability != null) return capability;

                var precondition = await DarlingRuntimePrecondition.StatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_column_stats");

                if (precondition != null) return precondition;

                coverage = await DarlingPgColumnStatsReader.GetCoverageVerdictAsync(
                    postgres, resolved.ServerId, windowEnd, rows.Count);

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
                postgres, resolved.ServerId, windowEnd, rows.Count);

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
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL column stats failed: {ex.Message}");
        }
    }
}
