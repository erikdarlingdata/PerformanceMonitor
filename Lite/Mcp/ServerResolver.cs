using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
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
    /// One server a name answers to, for a write that must not guess which one the caller meant (#4734).
    /// <paramref name="ServerName"/> is the STORAGE name (<see cref="RemoteCollectorService.GetServerNameForStorage"/>:
    /// the database and <c>:RO</c> suffixes included), the identity <paramref name="ServerId"/> is hashed from and
    /// the name a caller can pass back to pick exactly this registration.
    /// </summary>
    internal sealed record ServerCandidate(int ServerId, string ServerName, string DisplayName);

    /// <summary>
    /// What <see cref="MatchCandidates"/> found: every server the name answers to, and the rule that found them
    /// (<c>exact</c>, <c>partial</c>, or <c>none</c> when there are no candidates).
    /// </summary>
    internal sealed record CandidateMatch(IReadOnlyList<ServerCandidate> Candidates, string MatchedBy);

    /// <summary>
    /// Every enabled server a name answers to, for a WRITE (#4734: <c>mute_analysis_finding</c>). <see cref="Resolve"/>
    /// takes the first match, which is right for a read (a wrong sibling shows its name in the payload and the caller
    /// re-asks) and wrong for a write that persists a row against whichever server the name landed on. This is the
    /// same rule Darling's <c>mute_analysis_finding</c> applies: every EXACT match (case-insensitive) on the server
    /// name, the display name or the storage name if there is at least one, otherwise every PARTIAL match
    /// (<c>Contains</c>, case-insensitive) on the server name or display name. The storage name is an exact key so a
    /// candidate's own <c>server</c> value, as listed in an <c>ambiguous</c> answer, selects that registration when the
    /// caller passes it back; the read rule does not know it and is left alone.
    ///
    /// <para><b>Servers are counted by storage identity.</b> Two entries that share one storage name
    /// (<see cref="RemoteCollectorService.GetServerNameForStorage"/>) hash to one server_id, so they are ONE candidate;
    /// two registrations of one machine that differ in database or read-only intent are two.</para>
    ///
    /// <para><b>Pure.</b> It takes the enabled-server list and the name, so the rule unit-tests without a
    /// <see cref="ServerManager"/>. A null or blank name has no candidates (<c>MatchedBy</c> <c>none</c>); the caller that
    /// wants every server omits the name instead. Surrounding whitespace on a name is ignored.</para>
    /// </summary>
    internal static CandidateMatch MatchCandidates(IReadOnlyList<ServerConnection> servers, string? serverName)
    {
        var name = (serverName ?? string.Empty).Trim();
        if (name.Length == 0)
        {
            return new CandidateMatch(Array.Empty<ServerCandidate>(), "none");
        }

        var exact = DistinctByStorageName(servers.Where(s =>
            string.Equals(s.ServerName, name, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(s.DisplayName, name, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(RemoteCollectorService.GetServerNameForStorage(s), name, StringComparison.OrdinalIgnoreCase)));

        if (exact.Count > 0)
        {
            return new CandidateMatch(exact, "exact");
        }

        var partial = DistinctByStorageName(servers.Where(s =>
            s.ServerName.Contains(name, StringComparison.OrdinalIgnoreCase) ||
            (s.DisplayName?.Contains(name, StringComparison.OrdinalIgnoreCase) ?? false)));

        return new CandidateMatch(partial, partial.Count == 0 ? "none" : "partial");
    }

    /// <summary>One candidate per distinct storage name, in the order the enabled-server list holds them.</summary>
    private static List<ServerCandidate> DistinctByStorageName(IEnumerable<ServerConnection> matches)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var candidates = new List<ServerCandidate>();
        foreach (var server in matches)
        {
            var storageName = RemoteCollectorService.GetServerNameForStorage(server);
            if (seen.Add(storageName))
            {
                candidates.Add(new ServerCandidate(
                    RemoteCollectorService.GetDeterministicHashCode(storageName),
                    storageName,
                    server.DisplayName));
            }
        }

        return candidates;
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

    private static string ListAvailableServers(ServerManager serverManager) =>
        ListAvailableServers(serverManager.GetEnabledServers());

    /// <summary>
    /// The listing a miss carries, over a list the caller already holds. <c>internal</c> so
    /// <c>mute_analysis_finding</c>'s <c>not_found</c> answer (#4734) names the servers in the read tools' own words
    /// without a second read of the registry.
    /// </summary>
    internal static string ListAvailableServers(IReadOnlyList<ServerConnection> servers)
    {
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
