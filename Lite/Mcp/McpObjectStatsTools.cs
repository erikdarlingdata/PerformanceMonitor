using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpObjectStatsTools
{
    /// <summary>Lite's default result cap for get_table_index_sizes (the tool takes no top parameter) — the same 100 Darling uses.</summary>
    private const int TableSizesTop = 100;

    /// <summary>get_index_usage's default row cap — the same 200 Darling uses, now the caller's <c>limit</c> default rather than a hardcoded server-wide cap (#2636).</summary>
    private const int IndexUsageTop = 200;

    [McpServerTool(Name = "get_table_index_sizes"), Description("Gets the 100 largest tables with per-table size, growth (7d/30d/daily rate), and row counts from the latest daily snapshot. Indexes are rolled up per table. Use to find storage hot-spots and fast-growing tables for capacity planning. Growth is measured only over history the store actually holds: the history block says how many days of snapshots exist and whether the 7-day and 30-day baselines are reachable; growth_7d_mb / growth_30d_mb / growth_pct_30d are null (with the reason in growth_note) when their baseline does not exist, never re-labelled from a nearer one, and growth_over_available_history_* always spans exactly growth_window_days. A table absent from a baseline snapshot (created since) reports null growth for that window, not 0. tables_returned and truncated bound the page.")]
    public static async Task<string> GetTableIndexSizes(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            /* Over-fetch by one so truncation is observed, not inferred from a full page (#3541 A3's rule). */
            var rows = await dataService.GetObjectSizeGrowthAsync(resolved.ServerId, TableSizesTop + 1);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "index_object_stats")
                    ?? McpHelpers.Status("unavailable", "No object size data available. Index/object stats are collected daily.");
            }

            var truncated = rows.Count > TableSizesTop;
            var page = rows.Take(TableSizesTop).ToList();

            /* The store's span is one fact for every row (the boundaries CTE), so it is published once. */
            var span = page[0];
            var covers7d = span.Snapshot7dTime is not null;
            var covers30d = span.Snapshot30dTime is not null;

            var result = page.Select(r => new
            {
                database_name = r.DatabaseName,
                schema_name = r.SchemaName,
                table_name = r.TableName,
                reserved_mb = r.CurrentReservedMb,
                used_mb = r.CurrentUsedMb,
                total_rows = r.TotalRows,
                index_count = r.IndexCount,
                /* Each nominal-window figure comes from exactly the baseline it names, or is null (#3541
                   A12). The SQL this replaced folded a missing 30-day baseline onto the 7-day one and a
                   missing 7-day one onto the oldest, and labelled the result with the window asked for. */
                growth_7d_mb = r.Growth7dMb,
                growth_30d_mb = r.Growth30dMb,
                growth_pct_30d = r.GrowthPct30d,
                /* The figure that is always honest: growth from the store's earliest snapshot of this table
                   to its latest, over exactly growth_window_days. Null only when there is no span at all. */
                growth_over_available_history_mb = r.GrowthOverAvailableHistoryMb,
                growth_over_available_history_pct = r.GrowthOverAvailableHistoryPct,
                growth_window_days = r.DaysOfData,
                daily_growth_rate_mb = r.DailyGrowthRateMb,
                growth_note = GrowthNote(r),
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                history = new
                {
                    earliest_snapshot = span.EarliestSnapshotTime.ToString("o"),
                    latest_snapshot = span.LatestSnapshotTime.ToString("o"),
                    history_days_available = span.DaysOfData,
                    covers_7d = covers7d,
                    covers_30d = covers30d,
                    note = covers30d
                        ? null
                        : $"The store holds {span.DaysOfData} day(s) of index snapshots for this server, so the "
                          + (covers7d ? "30-day baseline does not exist: growth_30d_mb and growth_pct_30d are null" : "7-day and 30-day baselines do not exist: growth_7d_mb, growth_30d_mb and growth_pct_30d are null")
                          + " rather than re-measured over a shorter span under the same name. Read growth_over_available_history_* — it spans exactly growth_window_days.",
                },
                tables_returned = page.Count,
                truncated,
                tables = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_table_index_sizes", ex);
        }
    }

    /// <summary>
    /// Why a row's growth figures are null, when they are (#3541 A12): the store has no snapshot old enough
    /// for the window, or the snapshot exists but this table was not in it (created since), or there is no
    /// span at all. Null when every figure is defined, so the common row carries no note. Darling's twin words
    /// it identically.
    /// </summary>
    internal static string? GrowthNote(ObjectSizeGrowthBaselineRow r)
    {
        var notes = new List<string>();
        if (r.DaysOfData < 1)
            notes.Add("the store holds a single day of snapshots for this server, so no growth is knowable yet — every growth figure is null, not 0");
        if (r.Snapshot7dTime is null)
            notes.Add("no snapshot 7+ days old exists, so growth_7d_mb is null");
        else if (r.ReservedMb7dAgo is null)
            notes.Add($"this table was not in the {r.Snapshot7dTime:o} snapshot (created since), so growth_7d_mb is null — its whole current size is newer than 7 days");
        if (r.Snapshot30dTime is null)
            notes.Add("no snapshot 30+ days old exists, so growth_30d_mb and growth_pct_30d are null");
        else if (r.ReservedMb30dAgo is null)
            notes.Add($"this table was not in the {r.Snapshot30dTime:o} snapshot (created since), so growth_30d_mb and growth_pct_30d are null");
        else if (r.ReservedMb30dAgo <= 0)
            notes.Add("the table was empty 30 days ago, so growth_pct_30d has no denominator and is null (growth_30d_mb carries the absolute)");
        if (r.DaysOfData >= 1 && r.ReservedMbOldest is null)
            notes.Add($"this table was not in the earliest snapshot ({r.EarliestSnapshotTime:o}), so growth_over_available_history_* and daily_growth_rate_mb are null");
        else if (r.DaysOfData >= 1 && r.ReservedMbOldest <= 0)
            notes.Add("the table was empty at the earliest snapshot, so growth_over_available_history_pct has no denominator and is null");
        return notes.Count == 0 ? null : string.Join("; ", notes) + ".";
    }

    [McpServerTool(Name = "get_index_usage"), Description("Per-index usage (seeks, scans, lookups, updates) from the latest daily snapshot, classed Unused, Write-only, or Active. Unused/write-only sort first as drop candidates: on a server with many, results can be one database's unused indexes, hiding Active ones elsewhere. Counters reset at the last restart or index rebuild, so Write-only means no reads since then. last_user_access is UTC (de-skewed): compare directly with get_collection_log and list_servers. <<GUIDE>> Gets per-index usage (seeks, scans, lookups, updates) from the latest daily snapshot, classifying each index as Unused, Write-only, or Active. Unused and write-only indexes are listed FIRST because they are drop candidates - which means that on a server with many unused indexes the row limit can be filled entirely by one database's unused indexes, hiding every Active index elsewhere. Pass database_name to ask about one database, which is almost always what you want; the response carries matching_index_count and truncated so a short answer is never mistaken for an absent one. Counters are cumulative since the last instance restart. last_user_access is UTC - the underlying sys.dm_db_index_usage_stats columns are in the monitored server's local clock and this read de-skews them - so it compares directly against get_collection_log and list_servers. Classification: Unused is zero seeks, scans and lookups AND zero updates; Write-only is zero of the first three but at least one update; everything else is Active. sys.dm_db_index_usage_stats also clears on an index rebuild and on a database detach/reattach, not only on an instance restart, so a heavily-used index that was just rebuilt can read as Unused until it accrues new activity - check the index's maintenance history before treating an Unused row as a drop candidate. Empty results are two different things here. If database_name is given and matches no rows there, but the server has index data in other databases, status is empty, not unavailable, with a note to check the name against get_database_sizes. If nothing matches anywhere on the server, whether or not database_name was given, the result is not_collected (if the engine's collection state says so) or unavailable instead - this tool never reports a truly empty server as empty. The note field says whether the answer is complete or truncated. Truncated means more indexes matched than the cap returned; every Active index that did not fit is among the omitted rows, since Active is never returned ahead of an Unused or write-only one, and if unused/write-only indexes alone outnumber the cap, none of them appear either - a truncated, all-Unused answer says nothing about how many Active indexes exist.")]
    public static async Task<string> GetIndexUsage(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Limit to one database. Strongly recommended: without it, unused-first ordering can fill the whole result from one database.")] string? database_name = null,
        [Description("Maximum rows to return. Default 200.")] int limit = IndexUsageTop)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var database = string.IsNullOrWhiteSpace(database_name) ? null : database_name;

            /* The stamps below are THIS server's local wall clock in the store, so putting them in the
               naive-UTC frame every other field on this payload uses needs THIS server's offset, not the
               desktop tab's. See McpServerLocalWindow. De-skewed HERE and not inside LocalDataService
               because the WPF grids read the same rows and render them through ServerTimeHelper — that
               surface has its own frame defect and its own issue, and folding the two together would fix
               one by breaking the other. */
            var utcOffsetMinutes = await McpServerLocalWindow.OffsetForAsync(dataService, resolved.ServerId);

            var rows = await dataService.GetIndexUsageAsync(resolved.ServerId, limit, database);
            if (rows.Count == 0)
            {
                /* #2636: a database filter that matches nothing is a DIFFERENT answer from a server that
                   collects no index stats at all — the capability check still runs first (a wrong-engine
                   target has no index_object_stats at all), and only then does the filter get blamed for
                   its own empty result. */
                if (database is not null)
                {
                    var anyOnServer = await dataService.GetIndexUsageMatchCountAsync(resolved.ServerId);
                    if (anyOnServer > 0)
                    {
                        return McpHelpers.Status(
                            "empty",
                            $"No index usage rows for database '{database}' on {resolved.ServerName} at the "
                            + $"latest snapshot, though the server has {anyOnServer:N0} across its other "
                            + "databases. Check the database name against get_database_sizes — the filter "
                            + "matches exactly, and an excluded or renamed database looks identical to one "
                            + "with no indexes.");
                    }
                }

                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "index_object_stats")
                    ?? McpHelpers.Status("unavailable", "No index usage data available. Index/object stats are collected daily.");
            }

            /* Counted BEFORE the cap, by a second query. A count taken over the returned rows is a count of
               the page, which is the whole defect this answers. */
            var matching = await dataService.GetIndexUsageMatchCountAsync(resolved.ServerId, database);
            var truncated = matching > rows.Count;

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                schema_name = r.SchemaName,
                table_name = r.TableName,
                index_name = r.IndexName,
                index_type = r.IndexTypeDesc,
                classification = r.Classification,
                reserved_mb = r.ReservedMb,
                total_rows = r.TotalRows,
                user_seeks = r.UserSeeks,
                user_scans = r.UserScans,
                user_lookups = r.UserLookups,
                total_reads = r.TotalReads,
                user_updates = r.UserUpdates,
                last_user_access = r.LastUserAccess?.AddMinutes(-utcOffsetMinutes).ToString("o")
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                database_name = database,
                returned_index_count = rows.Count,
                matching_index_count = matching,
                truncated,
                note = truncated
                    ? $"TRUNCATED: {matching:N0} indexes match and {rows.Count:N0} were returned. Rows are "
                      + "ordered UNUSED FIRST across the whole server, so the ones omitted are the ACTIVE "
                      + "indexes and they may be concentrated in databases with no rows here at all. This is "
                      + "not evidence that a database was not collected — pass database_name to ask about "
                      + "one, or raise limit."
                    : "Complete: every index matching this filter at the latest snapshot is included.",
                indexes = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_index_usage", ex);
        }
    }

    [McpServerTool(Name = "get_object_locking"), Description("Gets per-index locking and latch contention (row/page lock waits in ms, lock escalations, page-latch and page-IO-latch waits) from the latest daily snapshot, top contended objects first. Use to find tables/indexes driving blocking and contention. Counters are cumulative since the last instance restart. LATEST IS A TIME: this reads the newest index/object snapshot for the server, not a window, and captured_at is the instant it was collected - these are the databases and indexes that existed AT that stamp, and because object stats are collected DAILY the stamp can be most of a day old on a healthy server and older still on one whose collector has stalled.")]
    public static async Task<string> GetObjectLocking(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetIndexLockingAsync(resolved.ServerId);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "index_object_stats")
                    ?? McpHelpers.Status("unavailable", "No locking/contention data recorded. Index/object stats are collected daily.");
            }

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                schema_name = r.SchemaName,
                table_name = r.TableName,
                index_name = r.IndexName,
                index_type = r.IndexTypeDesc,
                reserved_mb = r.ReservedMb,
                total_rows = r.TotalRows,
                row_lock_wait_count = r.RowLockWaitCount,
                row_lock_wait_ms = r.RowLockWaitInMs,
                page_lock_wait_count = r.PageLockWaitCount,
                page_lock_wait_ms = r.PageLockWaitInMs,
                lock_escalations = r.IndexLockPromotionCount,
                page_latch_wait_ms = r.PageLatchWaitInMs,
                page_io_latch_wait_ms = r.PageIoLatchWaitInMs
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* #3880: captured_at, the #3637 census's one spelling for a latest read's stamp - see
                   McpServerInfoTools.GetServerProperties for why it is a cut-over and not an alias. Erik's
                   ruling on the call Darling's twin recorded in #3878/#3879 (stamp it, do not roster it as
                   unstamped debt), taken on BOTH SKUs in one lane: the two get_object_locking bodies mirror
                   each other field-for-field, and #3876/#3877 fixed this half's anchor first. Every row
                   comes from ONE capture, so rows[0] is the stamp for all of them. */
                captured_at = rows[0].CollectionTime.ToString("o"),
                objects = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_object_locking", ex);
        }
    }
}
