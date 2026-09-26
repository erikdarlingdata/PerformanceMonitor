/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for autovacuum health, paired with the <c>pg_autovacuum_stats</c> collector — and, since
/// #3603, with the <c>autovacuum</c> family of <c>pg_log_events</c>, which is where each run's COST lives.
///
/// <para><b>Two sources, one page, by design.</b> <c>pg_stat_user_tables</c> says whether autovacuum is
/// keeping up: dead tuples against the table's own threshold, last-run stamps, run counts. It cannot say
/// what a run cost — that a hot table's every run takes forty minutes of I/O landing in the business peak
/// is invisible in a catalog that records only that it ran. <c>log_autovacuum_min_duration</c>'s completion
/// report carries exactly that (duration, pages and tuples removed, buffers read and dirtied, WAL), and the
/// log-event pipeline lifts it into columns (V130). This tool sets the two beside each other per table, so
/// "is it keeping up" and "what does it cost when it does" answer from one read, and the recommendation to
/// change a table's cost limit or naptime has the run history as its evidence rather than folklore.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgAutovacuumTools
{
    /// <summary>How many runs per table the history is READ over (the aggregates' denominator) and how many
    /// are PUBLISHED in detail. Twenty-five bounds the read on a table that ran every naptime for a day;
    /// five is what a reader looks at before asking for the log itself.</summary>
    internal const int RunsReadPerTable = 25;
    internal const int RunsShownPerTable = 5;

    /// <summary>
    /// Classifies a table by how far past its OWN threshold it is, not by its dead-tuple count. The same
    /// count is routine on a large table and urgent on a small one, so the ratio is the only comparable
    /// figure — and whether the pile is growing separates autovacuum losing a race from autovacuum not
    /// running at all.
    /// <para>The ratio handed in is the WORSE of the dead-tuple and insert-only ratios — the same GREATEST
    /// the read ranks by. Classifying from the dead ratio alone let the #1-ranked table, an append-only
    /// one far past its INSERT threshold, carry severity "ok" (#3534).</para>
    /// <para>Growth is nullable because a one-sample window cannot measure it: null never escalates, and
    /// never reads as "flat" either.</para>
    /// </summary>
    internal static string Classify(bool autovacuumDisabled, double? thresholdRatio, bool? deadTuplesGrowing)
    {
        /* A configuration finding, and the one case where the count is beside the point: this table will
           never be vacuumed by autovacuum no matter how bad it gets, and it holds back the whole
           database's freeze horizon while it sits there. */
        if (autovacuumDisabled)
        {
            return "critical_autovacuum_disabled_on_table";
        }

        if (thresholdRatio is null)
        {
            return "unknown_no_threshold";
        }

        return thresholdRatio switch
        {
            /* Ten times past the line is not a busy table, it is a stuck one — most often autovacuum
               being cancelled repeatedly by conflicting locks, or starved of workers. */
            >= 10 => "critical_far_past_threshold",
            >= 2 when deadTuplesGrowing == true => "warning_past_threshold_and_growing",
            >= 2 => "warning_past_threshold",
            >= 1 => "info_at_threshold",
            _ => "ok",
        };
    }

    [McpServerTool(Name = "get_pg_autovacuum_health"), Description("Gets PostgreSQL per-table autovacuum health, ranked by how far past EACH TABLE'S OWN threshold it is, honoring per-table reloptions overrides. Flags autovacuum-disabled tables and the analyze backlog. recent_runs carries what each run COST, from log_autovacuum_min_duration; null means no run event in the window, and a null field inside a run means that version never printed the clause, never zero. autovacuum_disabled_count/past_threshold_count/growing_count count only the page, not the whole server. Empty is a genuine all-clear: only tables with pending work are recorded at all. <<GUIDE>> Gets PostgreSQL per-table autovacuum health: which tables are behind on vacuum or analyze, ranked by how far past each table's OWN trigger threshold it is. This ratio is the point of the tool - a dead-tuple count alone is not actionable, because autovacuum fires at autovacuum_vacuum_threshold + scale_factor * reltuples, so 500,000 dead tuples is routine on a 50-million-row table and urgent on a 10,000-row one. Thresholds honour per-table reloptions overrides, not just the server settings, since ALTER TABLE SET (autovacuum_*) is common on exactly the big hot tables where the global default is wrong. Also reports tables with autovacuum switched off entirely, whether dead tuples are still growing (autovacuum losing a race) or flat (autovacuum blocked or not running), and the analyze backlog that drives bad row estimates. Each table also carries recent_runs - what its automatic vacuums and analyzes COST, from the log_autovacuum_min_duration completion reports the log-event pipeline stores (pg_log_events, family autovacuum): per run the duration_ms, pages_removed / pages_remaining, tuples_removed / tuples_remaining, buffer_hits / buffer_misses / buffer_dirtied (read_mb / written_mb at the default 8 kB block), wal_records / wal_bytes, plus window aggregates (runs_counted, vacuum_runs, analyze_runs, total_duration_ms, max_duration_ms, total_read_mb, total_wal_bytes) over the newest 25 runs in the window with the newest 5 shown - so 'is it keeping up' (the catalog half) and 'what does each run cost' (the log half) answer from one place, and a table that is green here but takes forty minutes of I/O per run in the business peak is visible. recent_runs is null for a table with no run event in the window: the target's log_autovacuum_min_duration is -1 (off) or above the runs' durations, the log is not being read (get_pg_plan_capture_readiness judges readability), or the table simply did not run - run_history_note says which of those this read can tell. A null figure INSIDE a run means that PostgreSQL version did not print the clause on that line (an analyze has no pages or tuples; 16/17 print no WAL clause on an analyze), never zero. Works on any PostgreSQL target; collected per database. This is what bounds the page - read truncated to know whether the server held more tables with pending work than were returned; it is observed by fetching one row past this cap, never inferred from a full page.")]
    public static async Task<string> GetPgAutovacuumHealth(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze, used for the dead-tuple growth comparison. Default 24.")] int hours_back = 24,
        [Description("Maximum tables to return, worst first. Default 20. This is what bounds the page - read truncated to know whether more tables had pending work than were returned.")] int limit = 20,
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
            var now = windowEnd;
            /* #3653 (one vocabulary): the page cut is OBSERVED, not inferred. The reader is asked for one row
               past the cap and McpHelpers.BoundPage turns that into (page, truncated) — the #3594 dialect every
               honest page in the repository speaks. This replaced `limit_reached = tables.Count >= limit`, which
               read a server with exactly `limit` tables behind on vacuum as a cut page and told the caller to
               raise a limit that had nothing more to give. */
            var fetched = await DarlingPgAutovacuumReader.GetPgAutovacuumAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, limit + 1, cancellationToken);
            var (rows, truncated) = McpHelpers.BoundPage(fetched, limit);

            if (rows.Count == 0)
            {
                /* "No table on this server has dead tuples" is the healthy answer for a PostgreSQL target
                   and a fabricated one for a SQL Server target — the collector has never run there. Ask the
                   engine before making the claim (#2532). */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_autovacuum_stats", cancellationToken);
                if (gated != null)
                {
                    return gated;
                }

                return JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    hours_back,
                    status = "no_pending_maintenance",
                    finding = "No table on this server has dead tuples, pending analyze work, inserts since "
                            + "its last vacuum, or autovacuum disabled. The collector records only tables with "
                            + "pending work, so an empty result is the healthy case rather than missing data.",
                }, McpHelpers.JsonOptions);
            }

            /* #3603: the cost half, read once for every table on the page and attached per table below. The
               relation key is the parser's `schema.table`; the database rides along because one relation
               name can exist in several databases and the log names the database on every run. */
            /* One past RunsReadPerTable per relation, for the same reason as the page above: RecentRuns binds
               each table's runs to the cap and OBSERVES whether the log held more, instead of inferring it from
               a history that happened to be exactly RunsReadPerTable long. */
            var runs = await DarlingPgLogEventReader.GetAutovacuumRunsAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now,
                rows.Select(r => $"{r.SchemaName}.{r.TableName}").Distinct(StringComparer.Ordinal).ToList(),
                RunsReadPerTable + 1, cancellationToken);
            var runsByTable = runs
                .GroupBy(r => (Database: r.DatabaseName, Relation: r.RelationName))
                .ToDictionary(g => g.Key, g => g.OrderByDescending(r => r.OccurredAtUtc).ToList());

            var tables = rows.Select(r =>
            {
                var relation = $"{r.SchemaName}.{r.TableName}";
                /* Exact (database, relation) first; a run whose database the log did not name (a V129-era row
                   stored before the parser lifted it) matches on the relation alone rather than vanishing. */
                if (!runsByTable.TryGetValue((r.DatabaseName, relation), out var tableRuns))
                {
                    runsByTable.TryGetValue((null, relation), out tableRuns);
                }

                /* -1 is the collector's not-applicable sentinel, and a 0 threshold happens on a table that
                   has never been analyzed. Neither can produce a ratio, and inventing one would rank a
                   table we know nothing about above tables we have measured. */
                double? ratio = r.VacuumThreshold > 0
                    ? Math.Round((double)r.DeadTuples / r.VacuumThreshold, 2)
                    : null;
                double? analyzeRatio = r.AnalyzeThreshold > 0
                    ? Math.Round((double)r.ModsSinceAnalyze / r.AnalyzeThreshold, 2)
                    : null;
                /* The insert-side twin, mirroring the read's ORDER BY CASE: the -1 sentinel (a major
                   without autovacuum_vacuum_insert_threshold, or an unreadable inserts figure) produces
                   no ratio rather than a negative one. */
                double? insertRatio = r.InsertVacuumThreshold > 0 && r.InsertsSinceVacuum >= 0
                    ? Math.Round((double)r.InsertsSinceVacuum / r.InsertVacuumThreshold, 2)
                    : null;
                /* Severity comes from the axis the ranking already uses — GREATEST(dead, insert), NULLs
                   ignored, exactly as the read orders. Classifying from the dead ratio alone let an
                   append-only worst_table read "ok" (#3534). */
                double? worstRatio = ratio is null ? insertRatio
                    : insertRatio is null ? ratio
                    : Math.Max(ratio.Value, insertRatio.Value);
                /* One sample cannot measure growth: first == latest is the same reading, and a false
                   there converts into "autovacuum blocked or not running" territory the window cannot
                   support. Null, not false — and the change figure goes with it. */
                bool? growing = r.FirstSeenAt == r.MeasuredAt ? null : r.DeadTuples > r.FirstDeadTuples;

                return new
                {
                    database_name = r.DatabaseName,
                    table_name = $"{r.SchemaName}.{r.TableName}",
                    severity = Classify(r.AutovacuumDisabled, worstRatio, growing),
                    dead_tuples = r.DeadTuples,
                    vacuum_threshold = r.VacuumThreshold >= 0 ? r.VacuumThreshold : (long?)null,
                    /* The headline number: 1.0 means autovacuum should be triggering right now. */
                    threshold_ratio = ratio,
                    dead_tuples_growing = growing,
                    dead_tuple_change = growing is null ? null : (long?)(r.DeadTuples - r.FirstDeadTuples),
                    /* The window the growth claim is measured over, so a caller can see how much history
                       stands behind it — and that null growth means one sample, not missing data. */
                    first_seen_at = r.FirstSeenAt,
                    live_tuples = r.LiveTuples,
                    /* The analyze half. Stale statistics produce bad row estimates and bad plans, which is
                       a different symptom from bloat and gets missed because both come from one process. */
                    mods_since_analyze = r.ModsSinceAnalyze,
                    analyze_threshold = r.AnalyzeThreshold >= 0 ? r.AnalyzeThreshold : (long?)null,
                    analyze_threshold_ratio = analyzeRatio,
                    /* The append-only path, PG13+: a table with no dead tuples is invisible to the vacuum
                       rule, and a table never vacuumed is never frozen either. */
                    inserts_since_vacuum = r.InsertsSinceVacuum >= 0 ? r.InsertsSinceVacuum : (long?)null,
                    insert_vacuum_threshold = r.InsertVacuumThreshold >= 0 ? r.InsertVacuumThreshold : (long?)null,
                    /* threshold_ratio's insert-side sibling, so the figure severity ranks on is visible
                       when the dead-tuple ratio is not the one that put the table here. */
                    insert_threshold_ratio = insertRatio,
                    autovacuum_disabled = r.AutovacuumDisabled,
                    total_bytes = r.TotalBytes >= 0 ? r.TotalBytes : (long?)null,
                    total_gb = r.TotalBytes >= 0 ? Math.Round(r.TotalBytes / 1024.0 / 1024.0 / 1024.0, 2) : (double?)null,
                    last_autovacuum = r.LastAutovacuum,
                    last_vacuum = r.LastVacuum,
                    last_autoanalyze = r.LastAutoanalyze,
                    last_analyze = r.LastAnalyze,
                    autovacuum_count = r.AutovacuumCount,
                    /* Never autovacuumed AND past threshold is the strongest single signal that something
                       is preventing it, rather than that it has not got round to this table yet. */
                    never_autovacuumed = r.LastAutovacuum is null && r.AutovacuumCount == 0,
                    measured_at = r.MeasuredAt,
                    /* #3603: what this table's runs COST, from the log. Null when the window holds no run
                       event for it — run_history_note at the top says what that can mean. */
                    recent_runs = RecentRuns(tableRuns),
                };
            })
            .ToList();

            var tablesWithRuns = tables.Count(t => t.recent_runs is not null);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                status = "tables_with_pending_maintenance",
                /* The page's count under the page's name (#3594: a page count is never a total, and it is
                   spelled *_returned so a reader can tell it from one). */
                tables_returned = tables.Count,
                /* Page-scoped counts: each is computed over the rows the read's LIMIT let through, not
                   the server. truncated is the discriminator that makes that caveat actionable — when it is
                   true, the caller knows these are top-N figures and can raise the limit (the
                   get_pg_database_stats pattern). */
                autovacuum_disabled_count = tables.Count(t => t.autovacuum_disabled),
                past_threshold_count = tables.Count(t => t.threshold_ratio >= 1 || t.insert_threshold_ratio >= 1),
                growing_count = tables.Count(t => t.dead_tuples_growing == true),
                truncated,
                worst_table = tables[0].table_name,
                worst_severity = tables[0].severity,
                /* #3603: how much of the page the cost half could speak for, and — when it is none — the
                   reasons this read can and cannot separate. Said once here rather than per table. */
                tables_with_run_history = tablesWithRuns,
                run_history_note = tablesWithRuns > 0
                    ? $"recent_runs on {tablesWithRuns} of {tables.Count} tables is from the log_autovacuum_min_duration "
                      + "completion reports stored in pg_log_events (family autovacuum) over the same window; a "
                      + "table without it did not complete an automatic vacuum or analyze that the target logged "
                      + "in the window. Aggregates cover the newest " + RunsReadPerTable + " runs per table; runs "
                      + "lists the newest " + RunsShownPerTable + "."
                    : "No table on this page has an autovacuum run event in the window. Three things produce "
                      + "that and this read cannot tell them apart: log_autovacuum_min_duration is -1 (off) or "
                      + "higher than any run's duration on the target (get_pg_server_config carries it; "
                      + "get_pg_logging_audit judges it); the server log is not being read (get_pg_log_events "
                      + "with family autovacuum is the direct check, and its empty branch names the reason the "
                      + "collector recorded); or nothing on this page was vacuumed in the window, which for a "
                      + "table far past its threshold is itself the finding.",
                tables,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_pg_autovacuum_health", ex);
        }
    }

    /// <summary>
    /// The <c>recent_runs</c> block for one table (#3603): aggregates over the runs read (newest
    /// <see cref="RunsReadPerTable"/> in the window) and the newest <see cref="RunsShownPerTable"/> in
    /// detail, in the same per-run shape <c>get_pg_log_events</c> publishes. Null for no runs — the
    /// caller's note explains the null once, at the top.
    /// <para>Sums skip null members rather than treating them as zero, and say how many runs carried the
    /// figure: an analyze run has no tuples clause, so <c>total_tuples_removed</c> over a mixed history is
    /// a sum over the vacuum runs and <c>runs_with_tuples</c> says so.</para>
    /// </summary>
    internal static object? RecentRuns(IReadOnlyList<DarlingPgLogEventReader.PgAutovacuumRunRow>? runs)
    {
        if (runs is null || runs.Count == 0)
        {
            return null;
        }

        static long? Sum(IEnumerable<long?> values)
        {
            long total = 0;
            var any = false;
            foreach (var v in values)
            {
                if (v is null) continue;
                total += v.Value;
                any = true;
            }
            return any ? total : null;
        }

        /* #3653 (one vocabulary): the caller hands in up to RunsReadPerTable + 1 runs (the tool reads one past
           the cap per relation); the cap is applied HERE and the cut observed — `truncated` is true exactly when
           the log held a run this block did not count. This replaced `history_capped = runs.Count >=
           RunsReadPerTable`, the same `>= cap` inference #3594 named on the page tools, one layer down and
           against a collector constant rather than the caller's limit: a table with exactly 25 logged runs read
           as capped. Every aggregate below is over the bound page, so the denominator is still RunsReadPerTable
           at most. */
        var (page, truncated) = McpHelpers.BoundPage(runs, RunsReadPerTable);
        runs = page;

        var durations = runs.Where(r => r.DurationMs is not null).Select(r => r.DurationMs!.Value).ToList();
        var totalMisses = Sum(runs.Select(r => r.BufferMisses));
        var totalDirtied = Sum(runs.Select(r => r.BufferDirtied));

        return new
        {
            /* The denominator of every aggregate here — capped at RunsReadPerTable, and truncated says when
               the cap bit, so "25 runs" on a table that ran 200 times reads as a sample, not a count. */
            runs_counted = runs.Count,
            truncated,
            vacuum_runs = runs.Count(r => !r.IsAnalyze),
            analyze_runs = runs.Count(r => r.IsAnalyze),
            newest_run_at = runs[0].OccurredAtUtc,
            oldest_counted_run_at = runs[^1].OccurredAtUtc,
            total_duration_ms = durations.Count > 0 ? durations.Sum() : (long?)null,
            max_duration_ms = durations.Count > 0 ? durations.Max() : (long?)null,
            avg_duration_ms = durations.Count > 0 ? (long?)Math.Round(durations.Average()) : null,
            total_tuples_removed = Sum(runs.Select(r => r.TuplesRemoved)),
            runs_with_tuples = runs.Count(r => r.TuplesRemoved is not null),
            total_pages_removed = Sum(runs.Select(r => r.PagesRemoved)),
            total_read_mb = totalMisses is null ? null : (double?)Math.Round(totalMisses.Value * 8192.0 / 1024.0 / 1024.0, 2),
            total_written_mb = totalDirtied is null ? null : (double?)Math.Round(totalDirtied.Value * 8192.0 / 1024.0 / 1024.0, 2),
            total_wal_bytes = Sum(runs.Select(r => r.WalBytes)),
            runs = runs.Take(RunsShownPerTable).Select(r => new
            {
                occurred_at = r.OccurredAtUtc,
                database_name = r.DatabaseName,
                run = DarlingMcpPgLogEventTools.AutovacuumRunShape(new DarlingPgLogEventReader.PgLogEventMetricsRow(
                    r.RelationName, null, r.DurationMs, r.PagesRemoved, r.PagesRemaining, r.TuplesRemoved, r.TuplesRemaining,
                    r.BufferHits, r.BufferMisses, r.BufferDirtied, r.WalRecords, r.WalBytes, r.IsAnalyze)),
            }),
        };
    }
}
