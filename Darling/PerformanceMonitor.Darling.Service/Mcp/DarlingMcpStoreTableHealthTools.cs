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
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.FrontDoor;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The health of the monitoring store's OWN tables as an MCP read (#8787): which store table carries empty space, which
/// one autovacuum is behind on, which one would gain from a rewrite. <c>get_store_metrics</c> sizes hypertables,
/// aggregates, the payload dimensions and jobs; this adds the plain and partitioned tables and the empty-space estimate.
/// The read is <see cref="DarlingStoreTableHealthReader"/>, shared with the Darling Viewer.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreTableHealthTools
{
    /// <summary>Default rows returned.</summary>
    public const int DefaultTop = 50;

    /// <summary>The sentence every answer carries about what the empty-space figure is.</summary>
    internal const string EstimateNote =
        "estimated_empty_bytes and estimated_empty_share are ESTIMATES from table statistics (the extension-free estimate the "
        + "PostgreSQL target table bloat collector uses), not measurements. A measured figure needs the pgstattuple extension, "
        + "which this read never installs. A table with no usable statistics has a null estimate and a reason, never zero.";

    /// <summary>The sentence a tables answer carries when the statistics were never reset, so the per-day rates are null (#8823).</summary>
    internal const string RatesNote =
        "The statistics were never reset, so the counters are older than the last start (stats_since) and their age is not known. "
        + "The per-day rates (inserts_per_day, updates_per_day, deletes_per_day, turnover_per_day) are null instead of a figure that "
        + "would be too high. The counters themselves are real. A statistics reset gives rates from the next read after it.";

    /// <summary>The sentence every answer carries about the as-of fields: a part is read from the store at most once in
    /// <see cref="DarlingStoreTableHealthCache.Lifetime"/>, so the answer says when each part it returns was read.</summary>
    internal static readonly string AsOfNote = string.Create(CultureInfo.InvariantCulture,
        $"Each part (tables, indexes, partition index coverage, autovacuum settings) is read from the store's catalogs once when the service starts and then at most once every {DarlingStoreTableHealthCache.Lifetime.TotalMinutes:0} minutes, and held in memory between reads. A call after that time gets the held part at once, with its own as_of and age_seconds, and a new read of it runs behind the call. ")
        + "tables_as_of, indexes_as_of, partition_index_coverage_as_of and autovacuum_settings.as_of are when the store answered that read (UTC), and the matching age_seconds is how old it is as of this response. held_for_seconds is how long a read is held before a call starts a new read behind it.";

    /// <summary>The sentence a tables answer carries about the autovacuum fields.</summary>
    internal static readonly string AutovacuumNote = string.Create(CultureInfo.InvariantCulture,
        $"autovacuum_cancels_24h counts the autovacuums of a table's relations the store log saw cancelled in the {DarlingStoreTableHealthReader.CancelWindow.TotalHours:0} hours before the read (a TOAST table's for its owner, a chunk's or leaf's for its parent). ")
        + string.Create(CultureInfo.InvariantCulture,
            $"repeatedly_cancelled is at least {DarlingStoreTableHealthReader.RepeatedCancelMinimum} in that window. behind is a relation past its vacuum trigger (or its insert trigger, on a plain table) with no vacuum, automatic or manual, completed in the {DarlingStoreTableHealthReader.BehindWindow.TotalHours:0} hours before the read. ")
        + "behind_hours is the time since that last vacuum, or since the statistics began when there was none (behind_since_source last_vacuum or stats_since): the relation crossed its trigger some time after it, so it has been past the trigger for at most that long. "
        + "A relation with autovacuum turned off is never behind. Tables behind come first, then tables repeatedly cancelled, then rewrite candidates.";

    /// <summary>The sections the tool can return.</summary>
    internal static readonly string[] Sections = { "tables", "indexes", "all" };

    [FrontDoorClass(FrontDoorToolClass.FanOut)]
    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_store_table_health"), Description(
        "Gets the health of the monitoring STORE's OWN tables and indexes, not a monitored server's; no server_name. Per table: size "
        + "split, estimated empty space (an ESTIMATE, null with a reason when statistics are missing or stale, never zero), dead rows, "
        + "autovacuum distance, autovacuum cancels in the last 24 hours, whether autovacuum is behind (sorted first), change rates, "
        + "whether plain VACUUM can give the space back, and rewrite_candidate. Also the store's autovacuum settings and write budget. "
        + "Per index: size, scans, bloat estimate, validity; plus index coverage for partitioned parents. "
        + "<<GUIDE>> Also per table: plain, partitioned, hypertable and continuous aggregate kinds, the size split (heap, index, toast), visibility map coverage, insert, update, delete and HOT rates since the statistics were reset, last vacuum and analyze, autovacuum off, 7-day growth where store_metrics records it. Per index: scans, index-to-heap ratio, larger_than_heap and rarely_used. Reads the catalogs of the "
        + "monitoring store once when the service starts and then at most once every 5 minutes: each part (tables, indexes, partition index coverage, autovacuum settings) is held in memory between reads, and a call after the 5 minutes gets the held part at once while a new read runs behind it, "
        + "and tables_as_of, indexes_as_of, partition_index_coverage_as_of and autovacuum_settings.as_of say when the store answered it (UTC), with the matching age_seconds as of this response. "
        + "top, offset, candidates_only and section cut the held rows, so they never cause a store read. Hypertable chunks and the relations holding compressed data roll up to the hypertable "
        + "(chunk_count), a continuous aggregate to its view name, and a partitioned table's leaves to the parent (leaf_count); a "
        + "leaf or chunk is never its own row, and chunk or leaf indexes that are clones of one parent index are one index row. "
        + "estimated_empty_bytes / estimated_empty_share come from the same statistics estimate the PostgreSQL target table bloat "
        + "collector runs, and cover only relations with a heap of at least 1 MB and usable statistics: estimate_relations_usable of "
        + "estimate_relations says how much of a roll-up was estimated, and estimate_unavailable_reason says why a row has no "
        + "estimate (below_size_floor, never_analyzed, no_column_statistics, analyze_record_lost, stale_statistics, no_chunks). "
        + "It is NOT n_live_tup over n_live_tup plus n_dead_tup, which reads 100 percent after a vacuum. A measured figure needs "
        + "pgstattuple, which is never installed. past_rewrite_floors = estimated share at least 50 percent AND estimated empty bytes "
        + "at least 64 MiB. Plain VACUUM gives space back only by cutting the empty pages off the END of the file, and only when that run "
        + "reaches PostgreSQL's truncate minimum (the smaller of 1000 pages and a sixteenth of the file), holds at least half of the "
        + "empty bytes, and the table's vacuum_truncate option is not off. rewrite_candidate = past_rewrite_floors AND tail_occupied: a "
        + "live row sits in the window at the end of the file (the larger of that minimum and the pages half the estimated empty bytes "
        + "fill, at most 32768 pages per relation), or vacuum_truncate is off, so only a rewrite returns the space. For a roll-up the "
        + "emptiest relations are checked, at most 10: tail_relations_checked of tail_relations_eligible. tail_occupied is false only "
        + "when every eligible relation was checked and VACUUM can cut each one. It is null when it was not read, when a window was "
        + "over the cap, when a roll-up has more eligible relations than were checked, or when the read failed: the tail is then "
        + "unknown, rewrite_candidate is false, and past_rewrite_floors stays true. vacuum_trigger_rows is "
        + "threshold + scale_factor x rows from the table's own settings or the server's (capped by the max threshold on PostgreSQL 18): "
        + "compare it with dead_rows, because many dead rows below the trigger are not yet due; relations_past_vacuum_trigger counts "
        + "the relations of a roll-up that are. The churn counters are since the last statistics reset. stats_since_source is stats_reset "
        + "when stats_since is that reset: the per-day rates (inserts, updates, deletes, turnover_per_day = deletes per day over live rows) are "
        + "worked out from it. When the statistics were never reset it is postmaster_start: the counters then began BEFORE stats_since, "
        + "because statistics survive a restart, so their age is unknown, the rates are null and rates_note says why; the raw counters "
        + "stay. Nothing is recorded. rarely_used is only set when the statistics window is at least 7 days old, never for a "
        + "primary key or unique index. Index bloat is the extension-free estimate the target index bloat collector uses. "
        + "growth_bytes_7d is null for objects store_metrics does not record. "
        + "autovacuum_cancels_24h counts the cancelled autovacuums the store log holds for a table's relations in the 24 hours before the read, a TOAST table's counted for "
        + "the table that owns it and a chunk's or leaf's for its parent, with last_autovacuum_cancel the latest; repeatedly_cancelled is 2 or more. "
        + "behind = a relation past its vacuum trigger (or its insert trigger, on a plain table) has had no vacuum, automatic or manual, complete in the 24 hours before the read; "
        + "relations_past_trigger counts the relations past a trigger. behind_since is that relation's last vacuum, or when the statistics began when it has had none "
        + "(behind_since_source last_vacuum or stats_since), and behind_hours runs from it to the read: the trigger was crossed some time after, so it is an upper bound. "
        + "A relation with autovacuum off is never behind. Order: behind, then repeatedly_cancelled, then rewrite_candidate, then the empty-space order. "
        + "tables_behind and tables_repeatedly_cancelled count them. autovacuum_settings is the store server's settings as they apply (an autovacuum_* value of -1 falls back to the vacuum_* "
        + "or maintenance_work_mem value, and cost_limit_setting, cost_delay_setting and work_mem_setting name the one in force): autovacuum_on, max_workers, workers_running (autovacuum "
        + "workers in pg_stat_activity at the read), naptime_seconds, cost_limit, cost_delay_ms, work_mem_kb, and write_budget_mb_per_s, the most all autovacuum workers together may "
        + "dirty per second (cost_limit / vacuum_cost_page_dirty pages every cost_delay_ms; null when cost_delay_ms is 0, which is unthrottled). cancels_24h is every cancel in the window "
        + "and cancels_unresolved those whose relation no longer exists or is in another database, which count for no table. Which relation a running vacuum is on and how far it "
        + "has got (pg_stat_progress_vacuum) is not read: PostgreSQL shows it only to members of pg_read_all_stats, which the store's roles are not. "
        + "The reply is also held to the 32,768-byte response budget, whole rows only, "
        + "the same number of rows from each section: budget_truncated = true says rows were left off the end for the budget, budget_note says how many "
        + "fit, and has_more and next_offset read on (pass next_offset as offset). A read that goes to the store sizes every relation and index: allow several seconds on a "
        + "store with many chunks. A part that runs past its read limit is left out and named in tables_left_out_note or indexes_left_out_note, "
        + "and the other part is still returned. When only the partition index coverage read runs past it, the index rows are still returned and coverage_left_out_note names the coverage that was left out. When every part asked for ran past the limit, status is timed_out.")]
    public static Task<string> GetStoreTableHealth(
        NpgsqlDataSource postgres,
        [Description("How many rows to return per section, in the answer's order. Default 50, max 1000. The reply is also held to a byte budget: when rows were left off for it, budget_truncated is true and next_offset reads on.")] int top = DefaultTop,
        [Description("Return only rewrite candidates in the tables section. Default false.")] bool candidates_only = false,
        [Description("Which section: tables, indexes (the per-index rows and the partition leaf index coverage) or all. Default all.")] string? section = null,
        [Description("Rows to skip in each section before the page starts. Default 0. Use next_offset when has_more is true.")] int offset = 0,
        CancellationToken cancellationToken = default) =>
        GetStoreTableHealth(postgres, top, candidates_only, section, offset, McpResponseBudget.DefaultBytes, cancellationToken);

    /// <summary>
    /// The read with its response budget. The MCP tool passes <see cref="McpResponseBudget.DefaultBytes"/>. The web route passes null:
    /// the Store Health page asks for one section at a time and draws every row it asked for. <paramref name="indexSource"/> is the store
    /// the index reads use (the tool's own store when null): a test gives it a store whose statements trip the deadline. <paramref name="coverageSource"/> is
    /// the same for the partition index coverage read alone (the index source when null). <paramref name="cache"/> holds the reads
    /// (#8924); null is the process's cache for the store each part reads (<see cref="DarlingStoreTableHealthCache.For"/>), and a test
    /// hands in one over a clock it steps and reads that count their calls. top, offset, candidates_only and section cut the held rows,
    /// so two calls that differ only in those share one store read.
    /// </summary>
    internal static async Task<string> GetStoreTableHealth(
        NpgsqlDataSource postgres, int top, bool candidates_only, string? section, int offset, int? responseByteBudget, CancellationToken cancellationToken,
        NpgsqlDataSource? indexSource = null, NpgsqlDataSource? coverageSource = null, DarlingStoreTableHealthCache? cache = null)
    {
        var invalidTop = McpHelpers.ValidateTop(top, "top");
        if (invalidTop != null)
        {
            return invalidTop;
        }

        if (offset < 0)
        {
            return McpHelpers.Refusal("offset", $"Invalid offset value '{offset}'. Must be 0 or more.");
        }

        var invalidSection = McpHelpers.ValidateChoice(section, Sections, "section");
        if (invalidSection != null)
        {
            return invalidSection;
        }

        var chosen = string.IsNullOrWhiteSpace(section) ? "all" : section.Trim().ToLowerInvariant();
        var wantTables = chosen is "tables" or "all";
        var wantIndexes = chosen is "indexes" or "all";

        try
        {
            var result = new Dictionary<string, object?>
            {
                ["section"] = chosen,
                ["top"] = top,
                ["offset"] = offset,
                ["as_of_note"] = AsOfNote,
                ["held_for_seconds"] = (int)DarlingStoreTableHealthCache.Lifetime.TotalSeconds,
            };

            IReadOnlyList<DarlingStoreTableHealthReader.StoreTableHealthRow> tableRows = Array.Empty<DarlingStoreTableHealthReader.StoreTableHealthRow>();
            IReadOnlyList<DarlingStoreTableHealthReader.StoreIndexHealthRow> indexRows = Array.Empty<DarlingStoreTableHealthReader.StoreIndexHealthRow>();
            IReadOnlyList<DarlingStoreTableHealthReader.StoreIndexCoverageRow> coverageRows = Array.Empty<DarlingStoreTableHealthReader.StoreIndexCoverageRow>();

            string? tablesTimeout = null;
            string? indexesTimeout = null;
            string? coverageTimeout = null;

            if (wantTables)
            {
                IReadOnlyList<DarlingStoreTableHealthReader.StoreTableHealthRow> all = Array.Empty<DarlingStoreTableHealthReader.StoreTableHealthRow>();
                try
                {
                    var held = cache ?? DarlingStoreTableHealthCache.For(postgres);
                    var read = await held.GetTablesAsync(postgres, cancellationToken: cancellationToken);
                    all = read.Rows;
                    result["tables_as_of"] = read.ReadAtUtc;
                    result["tables_age_seconds"] = read.AgeSeconds(held.UtcNow());
                }
                catch (StoreTableHealthTimeoutException ex)
                {
                    /* The tables read ran past its limit. The index part may still answer, so the reply says the table part was left out. */
                    tablesTimeout = ex.Message;
                }

                if (tablesTimeout is not null)
                {
                    result["header"] = "The table part was left out: " + tablesTimeout;
                    result["tables_left_out_note"] = tablesTimeout;
                }
                else
                {
                    var summary = DarlingStoreTableHealthReader.Summarize(all);
                    tableRows = candidates_only ? all.Where(r => r.RewriteCandidate).ToList() : all;

                    var noEstimate = summary.WithoutEstimateByReason.Count == 0
                        ? "every table has a usable estimate"
                        : string.Join(", ", summary.WithoutEstimateByReason.Select(p =>
                            $"{p.Value} {DarlingStoreTableHealthReader.DescribeReason(p.Key)}"));
                    var pastFloorsUnread = all.Count(r => r.PastRewriteFloors && r.TailOccupied is null);
                    var header = string.Create(CultureInfo.InvariantCulture,
                        $"{CountText.Plural(summary.TableCount, "store table")}, {CountText.Grouped(summary.TotalBytes)} bytes in total, "
                        + $"{CountText.Grouped(summary.Behind)} behind on autovacuum, {CountText.Grouped(summary.RepeatedlyCancelled)} with autovacuum cancelled repeatedly in 24 hours, "
                        + $"{CountText.Plural(summary.RewriteCandidates, "rewrite candidate")}, "
                        + $"{CountText.Grouped(summary.WithoutEstimate)} without a usable empty-space estimate ({noEstimate}).")
                        + (pastFloorsUnread > 0
                            ? string.Create(CultureInfo.InvariantCulture, $" {CountText.Grouped(pastFloorsUnread)} past the empty-space floors with the end of the file not checked.")
                            : string.Empty);

                    result["header"] = header;
                    result["estimate_note"] = EstimateNote;
                    var ratesKnown = all.Count == 0 || all.All(r => r.RatesKnown);
                    result["rates_known"] = ratesKnown;
                    result["rates_note"] = ratesKnown ? null : RatesNote;
                    result["store_tables"] = summary.TableCount;
                    result["store_total_bytes"] = summary.TotalBytes;
                    result["tables_behind"] = summary.Behind;
                    result["tables_repeatedly_cancelled"] = summary.RepeatedlyCancelled;
                    result["autovacuum_note"] = AutovacuumNote;
                    try
                    {
                        var held = cache ?? DarlingStoreTableHealthCache.For(postgres);
                        var settings = await held.GetAutovacuumSettingsAsync(postgres, cancellationToken: cancellationToken);
                        result["autovacuum_settings"] = Settings(settings, held.UtcNow());
                    }
                    catch (StoreTableHealthTimeoutException ex)
                    {
                        result["autovacuum_settings_left_out_note"] = ex.Message;
                    }

                    result["rewrite_candidates"] = summary.RewriteCandidates;
                    result["tables_without_estimate"] = summary.WithoutEstimate;
                    result["without_estimate_by_reason"] = summary.WithoutEstimateByReason.ToDictionary(p => p.Key, p => p.Value);
                    result["without_estimate_reason_text"] = summary.WithoutEstimateByReason.ToDictionary(p => p.Key, p => DarlingStoreTableHealthReader.DescribeReason(p.Key));
                    result["candidates_only"] = candidates_only;
                }
            }

            if (wantIndexes)
            {
                try
                {
                    var source = indexSource ?? postgres;
                    var held = cache ?? DarlingStoreTableHealthCache.For(source);
                    var read = await held.GetIndexesAsync(source, cancellationToken: cancellationToken);
                    indexRows = read.Rows;
                    result["indexes_as_of"] = read.ReadAtUtc;
                    result["indexes_age_seconds"] = read.AgeSeconds(held.UtcNow());
                }
                catch (StoreTableHealthTimeoutException ex)
                {
                    indexesTimeout = ex.Message;
                }

                if (indexesTimeout is null)
                {
                    /* The index rows are in hand. A coverage read that runs past its limit leaves out only the coverage part. */
                    try
                    {
                        var source = coverageSource ?? indexSource ?? postgres;
                        var held = cache ?? DarlingStoreTableHealthCache.For(source);
                        var read = await held.GetIndexCoverageAsync(source, cancellationToken: cancellationToken);
                        coverageRows = read.Rows;
                        result["partition_index_coverage_as_of"] = read.ReadAtUtc;
                        result["partition_index_coverage_age_seconds"] = read.AgeSeconds(held.UtcNow());
                    }
                    catch (StoreTableHealthTimeoutException ex)
                    {
                        coverageTimeout = ex.Message;
                    }
                }

                var incomplete = coverageRows.Count(c => c.LeavesMissingIndex > 0 || c.LeavesInvalidIndex > 0);

                if (indexesTimeout is not null)
                {
                    result["index_header"] = "The index part was left out: " + indexesTimeout;
                    result["indexes_left_out_note"] = indexesTimeout;
                }
                else
                {
                    var indexCounts = string.Create(CultureInfo.InvariantCulture,
                        $"{CountText.Plural(indexRows.Count, "store index", "store indexes")}, {indexRows.Count(r => r.LargerThanHeap)} larger than their heap, {indexRows.Count(r => r.RarelyUsed)} rarely used, ");
                    result["index_header"] = coverageTimeout is null
                        ? indexCounts + string.Create(CultureInfo.InvariantCulture,
                            $"{indexRows.Count(r => !r.Valid)} invalid, and {CountText.Plural(incomplete, "partitioned-parent index", "partitioned-parent indexes")} with a leaf missing or invalid.")
                        : indexCounts + string.Create(CultureInfo.InvariantCulture, $"and {indexRows.Count(r => !r.Valid)} invalid. The partition index coverage was left out: ") + coverageTimeout;
                    result["indexes_total"] = indexRows.Count;
                    if (coverageTimeout is null)
                    {
                        result["partition_index_coverage_total"] = coverageRows.Count;
                    }
                    else
                    {
                        result["coverage_left_out_note"] = coverageTimeout;
                    }
                }
            }

            if ((tablesTimeout is not null || !wantTables) && (indexesTimeout is not null || !wantIndexes))
            {
                /* Every part that was asked for ran past its limit: there is nothing to return but the timeout. */
                return McpHelpers.Status("timed_out", tablesTimeout ?? indexesTimeout!, new { timeout_seconds = StorageCommandDeadlines.McpReadSeconds });
            }

            /* A page is the same number of rows from every section, so one offset reads on in all of them. */
            var remaining = Math.Max(tableRows.Count, Math.Max(indexRows.Count, coverageRows.Count)) - offset;
            var inScope = remaining > top ? top : Math.Max(0, remaining);

            string Reply(int pageRows)
            {
                var tablePage = tableRows.Skip(offset).Take(pageRows).ToList();
                var indexPage = indexRows.Skip(offset).Take(pageRows).ToList();
                var coveragePage = coverageRows.Skip(offset).Take(pageRows).ToList();
                var hasMore = tableRows.Count > offset + tablePage.Count
                    || indexRows.Count > offset + indexPage.Count
                    || coverageRows.Count > offset + coveragePage.Count;
                var budgetCut = pageRows < top && hasMore;

                if (wantTables)
                {
                    result["tables_returned"] = tablePage.Count;
                    result["truncated"] = tableRows.Count > offset + tablePage.Count;
                    result["tables"] = tablePage.Select(Table).ToList();
                }

                if (wantIndexes)
                {
                    result["indexes_returned"] = indexPage.Count;
                    result["indexes_truncated"] = indexRows.Count > offset + indexPage.Count;
                    result["indexes"] = indexPage.Select(Index).ToList();
                    if (coverageTimeout is null)
                    {
                        result["partition_index_coverage_returned"] = coveragePage.Count;
                        result["partition_index_coverage_truncated"] = coverageRows.Count > offset + coveragePage.Count;
                        result["partition_index_coverage"] = coveragePage.Select(Coverage).ToList();
                    }
                }

                result["budget_truncated"] = budgetCut;
                result["budget_note"] = budgetCut
                    ? McpResponseBudget.PageCutNote(inScope, pageRows, "the answer's order", "rows per section",
                        $"Call again with offset {offset + pageRows} for the next rows, or ask for one section at a time (section \"tables\" or \"indexes\").")
                    : null;
                result["has_more"] = hasMore;
                result["next_offset"] = hasMore ? offset + pageRows : null;
                return JsonSerializer.Serialize(result, McpHelpers.JsonOptions);
            }

            return responseByteBudget is int budget
                ? McpResponseBudget.FitPrefix(inScope, Reply, budget).Json
                : Reply(top);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_table_health", ex);
        }
    }

    private static object Table(DarlingStoreTableHealthReader.StoreTableHealthRow r) => new
    {
        schema_name = r.SchemaName,
        table_name = r.TableName,
        kind = r.Kind,
        total_bytes = r.TotalBytes,
        heap_bytes = r.HeapBytes,
        index_bytes = r.IndexBytes,
        toast_bytes = r.ToastBytes,
        estimated_rows = r.EstimatedRows,
        live_rows = r.LiveRows,
        chunk_count = r.ChunkCount,
        leaf_count = r.LeafCount,
        estimated_empty_bytes = r.EstimatedEmptyBytes,
        estimated_empty_share = Round(r.EstimatedEmptyShare, 4),
        estimate_relations = r.EstimateRelations,
        estimate_relations_usable = r.EstimateRelationsUsable,
        estimate_unavailable_reason = r.EstimateUnavailableReason,
        estimate_unavailable_text = r.EstimateUnavailableReason is null ? null : DarlingStoreTableHealthReader.DescribeReason(r.EstimateUnavailableReason),
        past_rewrite_floors = r.PastRewriteFloors,
        tail_occupied = r.TailOccupied,
        tail_relations_eligible = r.TailRelationsEligible,
        tail_relations_checked = r.TailRelationsChecked,
        tail_relations_occupied = r.TailRelationsOccupied,
        rewrite_candidate = r.RewriteCandidate,
        relpages = r.RelPages,
        relallvisible = r.RelAllVisible,
        relallfrozen = r.RelAllFrozen,
        pct_all_visible = Round(r.PctAllVisible, 2),
        dead_rows = r.DeadRows,
        vacuum_trigger_rows = Round(r.VacuumTriggerRows, 1),
        relations_past_vacuum_trigger = r.RelationsPastVacuumTrigger,
        inserts_since_vacuum = r.InsertsSinceVacuum,
        insert_trigger_rows = Round(r.InsertTriggerRows, 1),
        relations_past_insert_trigger = r.RelationsPastInsertTrigger,
        rows_changed_since_analyze = r.RowsChangedSinceAnalyze,
        last_vacuum = Iso(r.LastVacuum),
        last_autovacuum = Iso(r.LastAutovacuum),
        last_analyze = Iso(r.LastAnalyze),
        last_autoanalyze = Iso(r.LastAutoanalyze),
        autovacuum_disabled = r.AutovacuumDisabled,
        stats_since = Iso(r.StatsSince),
        stats_since_source = r.StatsSinceSource,
        stats_hours = Math.Round(r.StatsHours, 4, MidpointRounding.AwayFromZero),
        tuples_inserted = r.TuplesInserted,
        tuples_updated = r.TuplesUpdated,
        tuples_hot_updated = r.TuplesHotUpdated,
        tuples_newpage_updated = r.TuplesNewPageUpdated,
        tuples_deleted = r.TuplesDeleted,
        autovacuum_count = r.AutovacuumCount,
        autoanalyze_count = r.AutoanalyzeCount,
        hot_update_share = Round(r.HotUpdateShare, 4),
        inserts_per_day = Round(r.InsertsPerDay, 1),
        updates_per_day = Round(r.UpdatesPerDay, 1),
        deletes_per_day = Round(r.DeletesPerDay, 1),
        turnover_per_day = Round(r.TurnoverPerDay, 4),
        growth_bytes_7d = r.GrowthBytes7d,
        autovacuum_cancels_24h = r.AutovacuumCancels,
        last_autovacuum_cancel = Iso(r.LastAutovacuumCancel),
        repeatedly_cancelled = r.RepeatedlyCancelled,
        relations_past_trigger = r.RelationsPastTrigger,
        behind = r.Behind,
        behind_since = Iso(r.BehindSince),
        behind_since_source = r.BehindSinceSource,
        behind_hours = Round(r.BehindHours, 1),
    };

    private static object Settings(StoreHealthRead<DarlingStoreTableHealthReader.StoreAutovacuumSettings> read, DateTime utcNow)
    {
        var s = read.Rows;
        return new
        {
            as_of = read.ReadAtUtc,
            age_seconds = read.AgeSeconds(utcNow),
            autovacuum_on = s.AutovacuumOn,
            max_workers = s.MaxWorkers,
            workers_running = s.WorkersRunning,
            naptime_seconds = s.NaptimeSeconds,
            cost_limit = s.CostLimit,
            cost_limit_setting = s.CostLimitSetting,
            cost_delay_ms = Round(s.CostDelayMs, 2),
            cost_delay_setting = s.CostDelaySetting,
            work_mem_kb = s.WorkMemKb,
            work_mem_setting = s.WorkMemSetting,
            vacuum_cost_page_dirty = s.CostPageDirty,
            block_size = s.BlockSize,
            write_budget_mb_per_s = Round(s.WriteBudgetMBps, 1),
            cancels_24h = s.CancelsInWindow,
            cancels_unresolved = s.CancelsUnresolved,
        };
    }

    private static object Index(DarlingStoreTableHealthReader.StoreIndexHealthRow r) => new
    {
        schema_name = r.SchemaName,
        table_name = r.TableName,
        kind = r.Kind,
        index_name = r.IndexName,
        on_compressed_data = r.OnCompressedData,
        index_count = r.IndexCount,
        leaf_count = r.LeafCount,
        bytes = r.Bytes,
        heap_bytes = r.HeapBytes,
        index_to_heap = Round(r.IndexToHeapRatio, 4),
        idx_scan = r.IndexScans,
        stats_since = Iso(r.StatsSince),
        stats_since_source = r.StatsSinceSource,
        is_primary_key = r.IsPrimaryKey,
        is_unique = r.IsUnique,
        valid = r.Valid,
        larger_than_heap = r.LargerThanHeap,
        larger_than_heap_count = r.LargerThanHeapCount,
        rarely_used = r.RarelyUsed,
        estimated_reclaimable_bytes = r.EstimatedReclaimableBytes,
        estimated_bloat_share = Round(r.EstimatedBloatShare, 4),
        estimated_basis_bytes = r.EstimatedBasisBytes,
        estimate_indexes_usable = r.EstimateIndexesUsable,
        estimate_skipped_reason = r.EstimateSkippedReason,
    };

    private static object Coverage(DarlingStoreTableHealthReader.StoreIndexCoverageRow r) => new
    {
        schema_name = r.SchemaName,
        table_name = r.TableName,
        index_name = r.IndexName,
        parent_index_valid = r.ParentIndexValid,
        leaf_count = r.LeafCount,
        leaves_with_index = r.LeavesWithIndex,
        leaves_missing_index = r.LeavesMissingIndex,
        missing_leaves = r.MissingLeaves,
        leaves_invalid_index = r.LeavesInvalidIndex,
        invalid_leaves = r.InvalidLeaves,
    };

    /* Every figure is rounded here, where the reply is built, so a double never prints its float noise. */
    private static double? Round(double? value, int places) => value is double v ? Math.Round(v, places, MidpointRounding.AwayFromZero) : null;

    private static string? Iso(DateTime? value) => value?.ToString("O", CultureInfo.InvariantCulture);
}
