using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpObjectStatsTools
{
    [McpServerTool(Name = "get_table_index_sizes"), Description("Gets the largest tables with per-table size, growth (7d/30d/daily rate), and row counts from the latest daily snapshot. Indexes are rolled up per table. Use to find storage hot-spots and fast-growing tables for capacity planning.")]
    public static async Task<string> GetTableIndexSizes(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetObjectSizeGrowthAsync(resolved.ServerId);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "index_object_stats")
                    ?? McpHelpers.Status("unavailable", "No object size data available. Index/object stats are collected daily.");
            }

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                schema_name = r.SchemaName,
                table_name = r.TableName,
                reserved_mb = r.CurrentReservedMb,
                used_mb = r.CurrentUsedMb,
                total_rows = r.TotalRows,
                index_count = r.IndexCount,
                growth_7d_mb = r.Growth7dMb,
                growth_30d_mb = r.Growth30dMb,
                daily_growth_rate_mb = r.DailyGrowthRateMb,
                growth_pct_30d = r.GrowthPct30d
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                tables = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_table_index_sizes", ex);
        }
    }

    [McpServerTool(Name = "get_index_usage"), Description("Gets per-index usage (seeks, scans, lookups, updates) from the latest daily snapshot, classifying each index as Unused, Write-only, or Active. Unused and write-only indexes are listed first - these are drop candidates. Counters are cumulative since the last instance restart.")]
    public static async Task<string> GetIndexUsage(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetIndexUsageAsync(resolved.ServerId);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "index_object_stats")
                    ?? McpHelpers.Status("unavailable", "No index usage data available. Index/object stats are collected daily.");
            }

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
                last_user_access = r.LastUserAccess?.ToString("o")
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                indexes = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_index_usage", ex);
        }
    }

    [McpServerTool(Name = "get_object_locking"), Description("Gets per-index locking and latch contention (row/page lock waits in ms, lock escalations, page-latch and page-IO-latch waits) from the latest daily snapshot, top contended objects first. Use to find tables/indexes driving blocking and contention. Counters are cumulative since the last instance restart.")]
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
                objects = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_object_locking", ex);
        }
    }
}
