using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Mcp;

/// <summary>
/// Resolves a user-provided server name to a ServerConnection and DatabaseService instance.
/// Supports partial matching, case-insensitive, against ServerName and DisplayName.
/// </summary>
internal static class ServerResolver
{
    public readonly record struct ResolvedServer(string ServerName, ServerConnection Connection, DatabaseService Service);

    public static ResolvedServer? Resolve(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        string? serverName)
    {
        var servers = serverManager.GetAllServers();

        if (servers.Count == 0)
        {
            return null;
        }

        ServerConnection? match = null;

        if (string.IsNullOrWhiteSpace(serverName))
        {
            if (servers.Count == 1)
            {
                match = servers[0];
            }
            else
            {
                return null;
            }
        }
        else
        {
            /* Exact match first */
            match = servers.Find(s =>
                string.Equals(s.ServerName, serverName, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(s.DisplayName, serverName, StringComparison.OrdinalIgnoreCase));

            /* Partial match */
            match ??= servers.Find(s =>
                s.ServerName.Contains(serverName, StringComparison.OrdinalIgnoreCase) ||
                s.DisplayName.Contains(serverName, StringComparison.OrdinalIgnoreCase));
        }

        if (match == null)
        {
            return null;
        }

        var service = registry.GetOrCreate(match);
        var displayName = string.IsNullOrEmpty(match.DisplayName) ? match.ServerName : match.DisplayName;
        return new ResolvedServer(displayName, match, service);
    }

    public static string ListAvailableServers(ServerManager serverManager)
    {
        var servers = serverManager.GetAllServers();
        if (servers.Count == 0)
        {
            return "No servers are configured.";
        }

        var lines = servers.Select(s =>
        {
            var roTag = s.ReadOnlyIntent ? " [Read-Only]" : "";
            return string.IsNullOrEmpty(s.DisplayName) || s.DisplayName == s.ServerName
                ? $"{s.ServerName}{roTag}"
                : $"{s.DisplayName} ({s.ServerName}){roTag}";
        });

        return string.Join("\n", lines);
    }
}
