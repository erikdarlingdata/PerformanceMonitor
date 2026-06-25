using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpServerInventoryTools
{
    [McpServerTool(Name = "get_database_sizes"), Description("Gets database file sizes, space usage, auto-growth settings, and volume free space. Shows each database with its data and log files, used vs total space, and the underlying storage volume capacity.")]
    public static async Task<string> GetDatabaseSizes(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var rows = await resolved.Value.Service.GetFinOpsDatabaseSizeStatsAsync();
            if (rows.Count == 0)
                return McpHelpers.Status("unavailable", "No database size data available. The size collector may not have run yet.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                file_count = rows.Count,
                databases = rows
                    .GroupBy(r => r.DatabaseName)
                    .Select(g => new
                    {
                        database_name = g.Key,
                        total_size_mb = g.Sum(r => r.TotalSizeMb),
                        used_size_mb = g.Sum(r => r.UsedSizeMb),
                        recovery_model = g.First().RecoveryModelDesc,
                        compatibility_level = g.First().CompatibilityLevel,
                        state = g.First().StateDesc,
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

    [McpServerTool(Name = "get_server_properties"), Description("Gets SQL Server instance properties from collected data: edition, version, CPU count, physical memory, socket/core topology, HADR status, and clustering.")]
    public static async Task<string> GetServerProperties(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var connectionString = resolved.Value.Service.ConnectionString;
            var row = await DatabaseService.GetServerPropertiesLiveAsync(connectionString);

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                edition = row.Edition,
                engine_edition = row.EngineEdition,
                sql_version = row.SqlVersion,
                cpu_count = row.CpuCount,
                physical_memory_mb = row.PhysicalMemoryMb,
                socket_count = row.SocketCount,
                cores_per_socket = row.CoresPerSocket,
                is_hadr_enabled = row.IsHadrEnabled,
                is_clustered = row.IsClustered
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_server_properties", ex);
        }
    }
}
