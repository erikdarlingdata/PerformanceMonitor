using System;
using System.Collections.Generic;
using System.ComponentModel;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpDiscoveryTools
{
    [McpServerTool(Name = "list_servers"), Description("Lists all monitored SQL Server instances with their current status (Online/Offline). Use this first to see available servers before calling other tools. Use get_collection_health to verify collectors are running.")]
    public static string ListServers(ServerManager serverManager)
    {
        try
        {
            var servers = serverManager.GetAllServers();
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

                lines.Add($"- {display} [{statusText}]");
            }

            return string.Join("\n", lines);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("list_servers", ex);
        }
    }
}
