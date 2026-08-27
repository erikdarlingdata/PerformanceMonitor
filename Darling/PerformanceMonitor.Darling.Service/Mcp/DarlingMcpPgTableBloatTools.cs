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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for per-table bloat, paired with the <c>pg_table_bloat_stats</c> collector (#2542).
///
/// <para><b>Everything this tool does is in service of one rule: never present an estimate as a
/// measurement.</b> The number it reports is arithmetic over PostgreSQL's column-width statistics, not a
/// reading of the table, and the difference is the difference between "consider investigating this" and
/// "run VACUUM FULL on a production table". So the estimate is named <c>_estimate</c> in every field that
/// carries it, is SUPPRESSED rather than captioned when its inputs are missing, is downgraded when the
/// statistics behind it are stale, and always ships beside the exact command that would settle it.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgTableBloatTools
{
    /// <summary>
    /// The threshold, re-exported from the ONE place the rule lives so this tool's tests can pin it without
    /// reaching across projects. <see cref="DarlingPgTableBloatReader.StaleStatisticsChurnRatio"/> carries
    /// the measurement it was set from.
    /// </summary>
    internal const double StaleStatisticsChurnRatio = DarlingPgTableBloatReader.StaleStatisticsChurnRatio;

    /// <summary>
    /// The REASON the estimate may not be shown, or null when it is fit to publish.
    ///
    /// <para>The decision itself is <see cref="DarlingPgTableBloatReader.EstimateIsUnpublishable"/> — shared
    /// with the WPF grid, so the two surfaces cannot disagree about whether a number is publishable. What
    /// lives here is only the prose, because a grid cell has room for three words and this has room to say
    /// what to actually do about it.</para>
    ///
    /// <para>Any non-null return means the caller nulls the estimate fields rather than rendering them with
    /// the reason attached — a footnote under a large red percentage is not a safeguard.</para>
    /// </summary>
    internal static string? EstimateSuppressionReason(DarlingPgTableBloatReader.PgTableBloatRow r)
    {
        if (!DarlingPgTableBloatReader.EstimateIsUnpublishable(r))
        {
            return null;
        }

        if (r.EstimateUnavailable)
        {
            return "SUPPRESSED: the estimate has no basis for this table. Either the table has never been "
                 + "analyzed, or - much more likely in production - the monitoring login cannot SELECT from "
                 + "it, so pg_stats returned no column widths to compute from. pg_monitor does NOT confer "
                 + "SELECT on user tables, and in that state the estimator does not fail: it silently "
                 + "returns a large number. Measured against a pg_monitor-only role on a real target it "
                 + "reported 88.59% for a table whose true bloat was 0.50%. Grant pg_read_all_data "
                 + "(PostgreSQL 14+) or SELECT on the schema to the monitoring role, then re-read.";
        }

        if (r.LiveTuples < 0)
        {
            return "SUPPRESSED: this table has never been analyzed, so PostgreSQL does not know how many "
                 + "rows it holds (reltuples is -1, its explicit 'unknown'). There is no row count to "
                 + "compare the page count against. Run ANALYZE on the table and re-read.";
        }

        {
            var pct = r.LiveTuples > 0
                ? Math.Round((double)r.ModsSinceAnalyze / r.LiveTuples * 100, 1)
                : 0;
            return "SUPPRESSED: the column-width statistics this estimate depends on are STALE - "
                 + pct.ToString("0.#", CultureInfo.InvariantCulture) + "% of this table's rows have been "
                 + "modified since it was last analyzed. This is the estimator's worst failure mode and it "
                 + "is not a small one: on a test pair differing only in this, the stale table's estimate "
                 + "was 92.64% against a true 10.93%, an error of 81 percentage points, with nothing in the "
                 + "arithmetic to show it. Run ANALYZE on the table and re-read.";
        }
    }

    /// <summary>
    /// What to do about the number, once it is judged fit to show. The remedies are deliberately ordered
    /// least-destructive first, and <c>VACUUM FULL</c> is never the lead recommendation — it takes an
    /// ACCESS EXCLUSIVE lock for the duration and needs free space equal to the finished table.
    /// </summary>
    internal static string BloatFinding(decimal bloatPct, long bloatBytes, bool pgstattupleAvailable, string qualifiedName)
    {
        var confirm = pgstattupleAvailable
            ? $"pgstattuple is installed on this database, so the exact figure is one query away: SELECT * FROM pgstattuple('{qualifiedName}'). It reads the WHOLE relation, so run it deliberately rather than on a schedule."
            : $"pgstattuple is NOT installed on this database, so the exact figure is not reachable from here. CREATE EXTENSION pgstattuple, then SELECT * FROM pgstattuple('{qualifiedName}') - and note it reads the whole relation, which on a large table is expensive enough that it is why this collector estimates instead.";

        if (bloatPct < 20)
        {
            return "Estimated bloat is low. Nothing to do - and at this level the estimate's own error bar "
                 + "is a meaningful share of the number, so treat it as 'not a problem' rather than as a "
                 + "precise figure. " + confirm;
        }

        if (bloatPct < 50)
        {
            return "Estimated bloat is moderate. This is usually autovacuum keeping up but running behind a "
                 + "steady write rate rather than a problem needing intervention - check "
                 + "get_pg_autovacuum_health for this table first, because if autovacuum is being BLOCKED "
                 + "(a long transaction, an idle-in-transaction session, an orphaned replication slot) then "
                 + "reclaiming the space now only buys time until the same thing happens again. "
                 + "get_pg_xmin_horizon names which of those it is. " + confirm;
        }

        return "Estimated bloat is high - roughly "
             + DescribeBytes(bloatBytes) + " of the heap is estimated to be reclaimable. CONFIRM IT BEFORE "
             + "ACTING: this is arithmetic over column-width statistics, not a measurement of the table. "
             + confirm
             + " Once confirmed, the remedy order matters. Fix the CAUSE first (get_pg_xmin_horizon and "
             + "get_pg_autovacuum_health) or the space comes straight back. To reclaim it, prefer VACUUM - "
             + "which returns space to the table's free space map for reuse without a lock - or pg_repack, "
             + "which rewrites the table with only a brief lock. VACUUM FULL returns space to the operating "
             + "system but holds an ACCESS EXCLUSIVE lock for the entire rewrite, blocking every read and "
             + "write against the table, and needs free disk equal to the finished table plus its indexes. "
             + "It is the last option, not the first.";
    }

    /// <summary>
    /// The dead-tuple fraction — a NARROWER but genuinely measured quantity, from the server's own counters
    /// rather than our arithmetic, and available whatever the grants are. This is what the tool falls back
    /// to when the estimate is suppressed, so that a permissions gap degrades the answer instead of
    /// removing it.
    /// </summary>
    internal static string DeadTupleFinding(long liveTuples, long deadTuples)
    {
        if (liveTuples < 0)
        {
            return "This table has never been analyzed, so PostgreSQL has no row count to compare its dead "
                 + "tuples against.";
        }

        var total = liveTuples + deadTuples;
        if (total <= 0)
        {
            return "No live or dead tuples recorded - the table is empty or its statistics have not been "
                 + "gathered yet.";
        }

        var pct = Math.Round((double)deadTuples / total * 100, 2);
        return pct.ToString("0.##", CultureInfo.InvariantCulture) + "% of this table's tuples are dead. "
             + "This is MEASURED rather than estimated - it comes from the server's own counters and needs "
             + "no width model, so it is the number to trust when the bloat estimate is suppressed. It is "
             + "also NARROWER than bloat: it counts dead tuples awaiting vacuum, and NOT the free space a "
             + "completed vacuum already left behind and did not return to the operating system, which is "
             + "usually the larger share on a table that has been churning for a while.";
    }

    /// <summary>
    /// The row's severity band, computed HERE rather than in the browser — the web renderer's rule is that
    /// it never re-derives a band, so a grid can only colour a row if the read hands it one. This is what
    /// gives the web dashboard the same visual cue the WPF grid gets from its row-style triggers; without
    /// it the two front ends disagree about which rows look urgent, which was a review finding.
    ///
    /// <para><c>Unknown</c> for a SUPPRESSED estimate, deliberately, and it is the whole reason this is not
    /// a plain percentage band: the row has no trustworthy number, so colouring it by severity would assert
    /// exactly what the suppression denies. It matches the neutral grey the WPF grid paints for the same
    /// state.</para>
    /// </summary>
    internal static string BloatSeverity(DarlingPgTableBloatReader.PgTableBloatRow r)
    {
        if (DarlingPgTableBloatReader.EstimateIsUnpublishable(r))
        {
            return "Unknown";
        }

        return r.BloatPctEstimate >= 50m ? "Critical"
             : r.BloatPctEstimate >= 20m ? "Warning"
             : "Healthy";
    }

    /// <summary>Bytes for PROSE only; the payload carries raw <c>_bytes</c> and a numeric <c>_mb</c>.</summary>
    internal static string DescribeBytes(long bytes)
    {
        if (bytes < 0)
        {
            return "an unmeasured size";
        }

        if (bytes >= 1024L * 1024 * 1024)
        {
            return Math.Round(bytes / 1024.0 / 1024 / 1024, 2).ToString("0.##", CultureInfo.InvariantCulture) + " GB";
        }

        if (bytes >= 1024L * 1024)
        {
            return Math.Round(bytes / 1024.0 / 1024, 1).ToString("0.#", CultureInfo.InvariantCulture) + " MB";
        }

        return Math.Round(bytes / 1024.0, 0).ToString("0", CultureInfo.InvariantCulture) + " KB";
    }

    [McpServerTool(Name = "get_pg_table_bloat")]
    [Description(
        "PostgreSQL per-table bloat - the damage autovacuum lag causes, next to the cause chain "
        + "get_pg_autovacuum_health, get_pg_wraparound_risk and get_pg_xmin_horizon already report. THE BLOAT "
        + "FIGURE IS AN ESTIMATE, NOT A MEASUREMENT: it is arithmetic over PostgreSQL's column-width "
        + "statistics and never reads the table, which is what makes it cheap enough to collect hourly. "
        + "Against pgstattuple on real tables with current statistics it was within about 2 percentage "
        + "points; with STALE statistics it was wrong by 81 percentage points, so the tool suppresses the "
        + "number outright rather than captioning it when the statistics behind it are stale, when the table "
        + "has never been analyzed, or when the monitoring login cannot SELECT from the table (pg_monitor "
        + "alone does not confer that, and in that state the estimator silently returns large numbers). "
        + "Measured sizes, the dead-tuple fraction from the server's own counters, and the growth across the "
        + "window are always reported, so a suppressed estimate degrades the answer rather than removing it. "
        + "Every row ships the exact pgstattuple command that would settle the question. Collected hourly per "
        + "database on writers only, for tables of at least 1 MB.")]
    public static async Task<string> GetPgTableBloat(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 168 (seven days), which shows whether the waste is growing or being reclaimed.")] int hours_back = 168,
        [Description("Maximum tables to return, biggest estimated waste first. Default 25.")] int limit = 25,
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
            var end = windowEnd;
            var start = end.AddHours(-hours_back);
            var rows = await DarlingPgTableBloatReader.GetPgTableBloatAsync(
                postgres, resolved.ServerId, start, end, limit);

            if (rows.Count == 0)
            {
                return await EmptyAsync(postgres, resolved.ServerId, resolved.ServerName, hours_back, start, end);
            }

            var suppressedCount = rows.Count(r => EstimateSuppressionReason(r) != null);
            var trusted = rows.Where(r => EstimateSuppressionReason(r) == null).ToList();
            var trustedBloatBytes = trusted.Sum(r => Math.Max(r.BloatBytesEstimate, 0));
            var anyPgstattuple = rows.Any(r => r.PgstattupleAvailable);
            var permissionsSuspected = rows.Count(r => r.EstimateUnavailable);

            var tables = rows.Select(r =>
            {
                var suppression = EstimateSuppressionReason(r);
                var qualified = (r.SchemaName ?? "public") + "." + (r.TableName ?? "?");
                var heapGrowth = r.HeapBytes >= 0 && r.FirstHeapBytes >= 0
                    ? r.HeapBytes - r.FirstHeapBytes
                    : (long?)null;

                return new
                {
                    database = r.DatabaseName,
                    schema = r.SchemaName,
                    table = r.TableName,
                    /* Server-computed, because the browser never re-derives a band. Drives the web grid's
                       cell colour and matches what the WPF row-style triggers paint. */
                    severity = BloatSeverity(r),

                    /* MEASURED, always reported. These are pg_relation_size readings and are true whatever
                       the statistics or the grants look like. */
                    heap_bytes = r.HeapBytes,
                    heap_mb = r.HeapBytes >= 0 ? Math.Round(r.HeapBytes / 1024.0 / 1024, 2) : (double?)null,
                    toast_bytes = r.ToastBytes,
                    index_bytes = r.IndexBytes,
                    /* Beside the heap estimate, never inside it: the estimator models heap tuples and has
                       no model for TOAST, and pg_stattuple on a relation measures its heap and not its
                       TOAST either - so folding TOAST in would make the accuracy claim meaningless. */
                    toast_note = r.ToastBytes > 0
                        ? "This table has out-of-line TOAST storage. The bloat estimate covers the HEAP "
                        + "only - TOAST is reported here as a measured size because the estimator has no "
                        + "model for it, and a TOAST relation can be bloated independently of its heap."
                        : null,

                    /* THE ESTIMATE - nulled, not captioned, when it cannot be trusted. */
                    bloat_bytes_estimate = suppression is null ? r.BloatBytesEstimate : (long?)null,
                    bloat_mb_estimate = suppression is null
                        ? Math.Round(r.BloatBytesEstimate / 1024.0 / 1024, 2)
                        : (double?)null,
                    bloat_pct_estimate = suppression is null ? r.BloatPctEstimate : (decimal?)null,
                    estimate_is_an_estimate = true,
                    estimate_suppressed = suppression != null,
                    estimate_suppression_reason = suppression,
                    bloat_finding = suppression is null
                        ? BloatFinding(r.BloatPctEstimate, r.BloatBytesEstimate, r.PgstattupleAvailable, qualified)
                        : null,

                    /* MEASURED fallback, always reported - this is what a suppressed estimate degrades to
                       rather than to nothing. */
                    live_tuples = r.LiveTuples,
                    dead_tuples = r.DeadTuples,
                    dead_tuple_pct = r.LiveTuples >= 0 && r.LiveTuples + r.DeadTuples > 0
                        ? Math.Round((double)r.DeadTuples / (r.LiveTuples + r.DeadTuples) * 100, 2)
                        : (double?)null,
                    dead_tuple_finding = DeadTupleFinding(r.LiveTuples, r.DeadTuples),

                    /* The trust signals, reported whether or not they fired, so a reader can see WHY the
                       estimate was or was not published rather than having to take it on faith. */
                    mods_since_analyze = r.ModsSinceAnalyze,
                    mods_since_analyze_pct_of_rows = r.LiveTuples > 0
                        ? Math.Round((double)r.ModsSinceAnalyze / r.LiveTuples * 100, 1)
                        : (double?)null,
                    last_analyzed = r.LastAnalyzed?.ToString("o"),
                    fillfactor = r.FillFactor,
                    fillfactor_note = r.FillFactor < 100
                        ? "This table has a non-default fillfactor, so it RESERVES free space on purpose for "
                        + "HOT updates. The estimate accounts for that reservation; pgstattuple's free_space "
                        + "does not, and will therefore report a larger figure for the same healthy table. "
                        + "Measured on a fillfactor-70 table: 3.79% estimated against 32.52% from "
                        + "pgstattuple, and the estimate is the more useful reading of the two."
                        : null,

                    /* The estimator's own inputs, so the number can be argued with rather than believed. */
                    estimated_tuple_bytes = r.EstimatedTupleBytes,
                    estimated_heap_pages = r.EstimatedHeapPages,
                    heap_pages = r.HeapPages,
                    alignment_bytes = r.AlignmentBytes,

                    /* The escalation path, per table, with the command already written out. */
                    pgstattuple_available = r.PgstattupleAvailable,
                    exact_measurement_command = r.PgstattupleAvailable
                        ? $"SELECT * FROM pgstattuple('{qualified}');"
                        : $"CREATE EXTENSION pgstattuple; SELECT * FROM pgstattuple('{qualified}');",

                    /* The trend, on the MEASURED size, so it survives a suppressed estimate. */
                    heap_bytes_growth_in_window = heapGrowth,
                    bloat_bytes_estimate_growth_in_window = suppression is null
                        ? r.BloatBytesEstimate - r.FirstBloatBytesEstimate
                        : (long?)null,
                    sample_count = r.SampleCount,
                    first_seen_at = r.FirstSeenAt.ToString("o"),
                    measured_at = r.MeasuredAt.ToString("o"),
                };
            })
            .ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                status = "table_bloat",
                table_count = tables.Count,
                /* Named for what it is: a sum of ESTIMATES over the tables whose estimates were fit to
                   publish. Not "reclaimable bytes", which is a promise the arithmetic cannot make. */
                estimated_bloat_bytes_over_trusted_rows = trustedBloatBytes,
                estimated_bloat_mb_over_trusted_rows = Math.Round(trustedBloatBytes / 1024.0 / 1024, 2),
                trusted_estimate_count = trusted.Count,
                suppressed_estimate_count = suppressedCount,
                pgstattuple_available_anywhere = anyPgstattuple,
                limit_reached = tables.Count >= limit,
                /* Named at the top so it is read before any number below it. */
                figures_are_estimates_not_measurements = true,
                note = "Every bloat figure here is an ESTIMATE computed from PostgreSQL's column-width "
                     + "statistics; the table itself is never read, which is what makes hourly collection "
                     + "affordable. Sizes, tuple counts and the dead-tuple fraction ARE measured. Against "
                     + "pgstattuple on real tables the estimate was within about 2 percentage points where "
                     + "the statistics were current, and wrong by 81 percentage points where they were not "
                     + "- so estimates whose inputs cannot be trusted are suppressed rather than captioned, "
                     + "and each row carries the pgstattuple command that would settle it exactly."
                     + (permissionsSuspected > 0
                         ? $" {permissionsSuspected} table(s) have NO usable column statistics. The usual "
                         + "cause is permissions rather than a missing ANALYZE: pg_stats is filtered by "
                         + "SELECT privilege and pg_monitor does not grant it on user tables. Granting "
                         + "pg_read_all_data (PostgreSQL 14+) to the monitoring role fixes this for the "
                         + "whole instance; on PostgreSQL 13 that role does not exist and explicit GRANT "
                         + "SELECT is the only route."
                         : string.Empty)
                     + (tables.Count >= limit
                         ? $" The row limit of {limit} was REACHED, so the totals cover only the tables "
                         + "returned. Raise limit for the full picture."
                         : string.Empty),
                tables,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_pg_table_bloat", ex);
        }
    }

    /// <summary>
    /// Which KIND of nothing an empty result is. The engine question is asked FIRST (#2532): "no bloat" is a
    /// statement about a PostgreSQL instance and is simply false said about a SQL Server target.
    /// <para>The denominator is the DATA, on the same relation the read walks, because
    /// <c>pg_table_bloat_stats</c> is a PERIODIC surface — a row exists per qualifying table every cycle, so
    /// any stored sample proves somebody looked, and zero rows on a collected server means every table is
    /// under the 1 MB floor.</para>
    /// </summary>
    private static async Task<string> EmptyAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, int hoursBack, DateTime start, DateTime end)
    {
        var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
            postgres, serverId, serverName, "pg_table_bloat_stats");
        if (gated != null)
        {
            return gated;
        }

        var probe = await DarlingPgTableBloatReader.ProbePgTableBloatAsync(postgres, serverId, start, end);

        var hints = new
        {
            server = serverName,
            hours_back = hoursBack,
            rows_in_window = probe.RowsInWindow,
            snapshots_in_window = probe.SnapshotsInWindow,
            ever_collected = probe.RowsEver > 0,
        };

        if (probe.SnapshotsInWindow >= 1)
        {
            return McpHelpers.Status(
                "empty",
                $"No tables were recorded for {serverName} in the last {hoursBack} hour(s) even though "
                + "collection ran. Every table on every collected database is under the collector's 1 MB "
                + "floor, below which bloat is not an actionable amount of space. That is a genuine "
                + "all-clear rather than missing data.",
                hints);
        }

        return McpHelpers.Status(
            "unavailable",
            probe.RowsEver > 0
                ? $"No bloat snapshots were collected for {serverName} in the last {hoursBack} hour(s), so "
                + "the window says nothing either way. Collection HAS run for this server outside the "
                + "window: widen hours_back, or use get_collection_health to find where it stopped."
                : $"No bloat snapshots have EVER been collected for {serverName}, so there is nothing to "
                + "read and this is NOT an all-clear. Check that collection is running and enabled for this "
                + "server - and note this collector is gated OFF on read replicas, because the statistics "
                + "it needs to judge its own accuracy read as zero there.",
            hints);
    }
}
