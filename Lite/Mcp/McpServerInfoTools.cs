using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpServerInfoTools
{
    [McpServerTool(Name = "get_server_properties"), Description("Gets SQL Server instance properties: edition, version, CPU count, physical memory, socket/core topology, HADR status, and clustering. Use for capacity planning and edition-aware recommendations. LATEST IS A TIME: this reads the newest properties snapshot, not a window, and captured_at is the instant it was collected - a core count or memory figure here is what the server reported AT that stamp, and on a server whose collector has stalled the stamp is the only thing that says how stale it is.")]
    public static async Task<string> GetServerProperties(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var row = await dataService.GetLatestServerPropertiesAsync(resolved.ServerId);
            if (row == null)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "server_properties")
                    ?? McpHelpers.Status("unavailable", "No server properties available. The properties collector may not have run yet.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* #3653: captured_at, the #3637 census's one spelling for a latest read's stamp. This tool
                   stamped itself as collection_time before that vocabulary existed and was carried as a
                   named allowance on both SKUs; a cut-over rather than an alias, because the census is the
                   contract and a second key for one instant is the drift it exists to refuse. */
                captured_at = row.CollectionTime.ToString("o"),
                edition = row.Edition,
                engine_edition = row.EngineEdition,
                product_version = row.ProductVersion,
                product_level = row.ProductLevel,
                product_update_level = string.IsNullOrEmpty(row.ProductUpdateLevel) ? null : row.ProductUpdateLevel,
                cpu_count = row.CpuCount,
                hyperthread_ratio = row.HyperthreadRatio,
                socket_count = row.SocketCount,
                cores_per_socket = row.CoresPerSocket,
                physical_memory_mb = row.PhysicalMemoryMb,
                is_hadr_enabled = row.IsHadrEnabled,
                is_clustered = row.IsClustered,
                enterprise_features = string.IsNullOrEmpty(row.EnterpriseFeatures) ? null : row.EnterpriseFeatures,
                service_objective = string.IsNullOrEmpty(row.ServiceObjective) ? null : row.ServiceObjective
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_server_properties", ex);
        }
    }

    [McpServerTool(Name = "get_database_sizes"), Description("Gets database file sizes, space usage, and volume free space. Shows each database file with total size, used space, auto-growth settings, and the underlying volume's capacity. Use for capacity planning and identifying space pressure. LATEST IS A TIME: this reads the newest size snapshot, not a window, and captured_at is the instant it was collected - a volume's free space here is what it was AT that stamp, and a file that grew since is not reflected until the next collection.")]
    public static async Task<string> GetDatabaseSizes(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetLatestDatabaseSizeStatsAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "database_size_stats")
                    ?? McpHelpers.Status("unavailable", "No database size data available. The size collector may not have run yet.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* #3653: captured_at - see GetServerProperties above for why it is a cut-over, not an alias. */
                captured_at = rows[0].CollectionTime.ToString("o"),
                file_count = rows.Count,
                databases = rows
                    .GroupBy(r => r.DatabaseName)
                    .Select(g => new
                    {
                        database_name = g.Key,
                        total_size_mb = g.Sum(r => r.TotalSizeMb),
                        used_size_mb = g.Sum(r => r.UsedSizeMb),
                        files = g.Select(r => new
                        {
                            file_name = r.FileName,
                            file_type = r.FileTypeDesc,
                            total_size_mb = r.TotalSizeMb,
                            used_size_mb = r.UsedSizeMb,
                            auto_growth_mb = r.AutoGrowthMb,
                            max_size_mb = r.MaxSizeMb,
                            volume_mount_point = r.VolumeMountPoint,
                            volume_total_mb = r.VolumeTotalMb,
                            volume_free_mb = r.VolumeFreeMb
                        })
                    })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_database_sizes", ex);
        }
    }
}
