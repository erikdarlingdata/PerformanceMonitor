using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// Resolves a user-provided server name to a server_id for data queries.
/// Supports partial matching, case-insensitive, against ServerName and DisplayName.
/// </summary>
internal static class ServerResolver
{
    public static (int ServerId, string ServerName)? Resolve(
        ServerManager serverManager,
        string? serverName)
    {
        var servers = serverManager.GetEnabledServers();

        if (servers.Count == 0)
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(serverName))
        {
            if (servers.Count == 1)
            {
                var s = servers[0];
                var storageName = RemoteCollectorService.GetServerNameForStorage(s);
                return (RemoteCollectorService.GetDeterministicHashCode(storageName), storageName);
            }

            return null;
        }

        /* Exact match first */
        var exact = servers.Find(s =>
            string.Equals(s.ServerName, serverName, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(s.DisplayName, serverName, StringComparison.OrdinalIgnoreCase));

        if (exact != null)
        {
            var exactName = RemoteCollectorService.GetServerNameForStorage(exact);
            return (RemoteCollectorService.GetDeterministicHashCode(exactName), exactName);
        }

        /* Partial match */
        var partial = servers.Find(s =>
            s.ServerName.Contains(serverName, StringComparison.OrdinalIgnoreCase) ||
            s.DisplayName.Contains(serverName, StringComparison.OrdinalIgnoreCase));

        if (partial != null)
        {
            var partialName = RemoteCollectorService.GetServerNameForStorage(partial);
            return (RemoteCollectorService.GetDeterministicHashCode(partialName), partialName);
        }

        return null;
    }

    public static string ListAvailableServers(ServerManager serverManager)
    {
        var servers = serverManager.GetEnabledServers();
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
