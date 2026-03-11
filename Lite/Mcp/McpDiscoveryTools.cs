using System.ComponentModel;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpDiscoveryTools
{
    [McpServerTool(Name = "list_servers"), Description("Lists all monitored SQL Server instances with their current status and last collection time. Use this first to see available servers before calling other tools.")]
    public static async Task<string> ListServers(ServerManager serverManager, LocalDataService dataService)
    {
        var servers = serverManager.GetEnabledServers();
        if (servers.Count == 0)
        {
            return "No servers are configured.";
        }

        var lines = new List<string> { $"Monitored servers ({servers.Count}):\n" };
        foreach (var s in servers)
        {
            var roTag = s.ReadOnlyIntent ? " [Read-Only]" : "";
            var display = string.IsNullOrEmpty(s.DisplayName) || s.DisplayName == s.ServerName
                ? $"{s.ServerName}{roTag}"
                : $"{s.DisplayName} ({s.ServerName}){roTag}";

            var status = serverManager.GetConnectionStatus(s.Id);
            var statusText = status.IsOnline switch
            {
                true => "Online",
                false => "Offline",
                null => "Status not checked"
            };

            var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s));
            var summary = await dataService.GetServerSummaryAsync(serverId, s.DisplayNameWithIntent);
            var lastCollection = summary?.LastCollectionTime?.ToString("o") ?? "No data collected";

            lines.Add($"- {display} [{statusText}] (last collection: {lastCollection})");
        }

        return string.Join("\n", lines);
    }
}
