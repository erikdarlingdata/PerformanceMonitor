using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// Resolves a user-provided server name to a server_id for data queries.
/// Supports partial matching, case-insensitive, against ServerName and DisplayName.
/// </summary>
internal static class ServerResolver
{
    private static (int ServerId, string ServerName)? Resolve(
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

    /// <summary>
    /// Resolves a server name, returning either the resolved (server_id, name) or a ready-to-return
    /// error string listing the available servers. Lets MCP tools collapse the repeated resolve-and-bail
    /// block to: var (resolved, error) = ResolveOrError(...); if (error != null) return error;
    /// resolved is default (not meaningful) whenever error is non-null — always bail on error first.
    ///
    /// <para><b>The miss is the <c>invalid</c> envelope, not a sentence (#3739).</b> The string handed back is
    /// <see cref="McpHelpers.Refusal"/>'s <c>{"status":"invalid","message":"Could not resolve server. …",
    /// "hints":{"parameter":"server_name"}}</c> — the same bytes Darling's resolver builds for the same miss —
    /// so every tool that <c>return error;</c>s puts a status word on the wire without changing, and a client
    /// that keys on <c>status</c> (which the instructions teach it to) sees a refusal rather than prose. The
    /// sentence inside is byte-for-byte what it was; read it back through <see cref="McpHelpers.ErrorMessageOf"/>.</para>
    /// </summary>
    public static ((int ServerId, string ServerName) resolved, string? error) ResolveOrError(
        ServerManager serverManager,
        string? serverName)
    {
        var resolved = Resolve(serverManager, serverName);
        return resolved is null
            ? (default, McpHelpers.Refusal("server_name", $"Could not resolve server. Available servers:\n{ListAvailableServers(serverManager)}"))
            : (resolved.Value, null);
    }

    /// <summary>
    /// When the enabled server whose storage identity is <paramref name="serverId"/> was added
    /// (<see cref="Models.ServerConnection.RegisteredAtUtc"/>), or null when none matches (#3967). The resolver
    /// hands back an id and a name, and the summary read needs the registration too, so its band agrees with
    /// the Overview card's for the same server.
    /// </summary>
    internal static DateTime? RegisteredAtUtc(ServerManager serverManager, int serverId) =>
        serverManager.GetEnabledServers()
            .Where(s => RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s)) == serverId)
            .Select(s => (DateTime?)s.RegisteredAtUtc)
            .FirstOrDefault();

    private static string ListAvailableServers(ServerManager serverManager)
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
