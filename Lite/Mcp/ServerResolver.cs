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
    /// <summary>
    /// What a read tool's server name comes to over the enabled servers: the one server it names, or a refusal.
    /// The name goes through <see cref="MatchCandidates"/>, the rule the write tools use, so a name that several
    /// registrations answer to is refused with the candidates listed instead of landing on whichever the list
    /// holds first. That matters when one host holds several monitored databases (Azure SQL Database): they share
    /// one host name and differ only in database, so the host name alone cannot say which one the caller meant.
    /// A candidate's own <c>server</c> value (its storage name) selects exactly that registration.
    /// </summary>
    internal static ((int ServerId, string ServerName) resolved, string? error) ResolveIn(
        IReadOnlyList<ServerConnection> servers,
        string? serverName)
    {
        if (string.IsNullOrWhiteSpace(serverName))
        {
            /* No name: the only server there is, or nothing to pick from. */
            if (servers.Count == 1)
            {
                var s = servers[0];
                var storageName = RemoteCollectorService.GetServerNameForStorage(s);
                return ((RemoteCollectorService.GetDeterministicHashCode(storageName), storageName), null);
            }

            return (default, Miss(servers));
        }

        var match = MatchCandidates(servers, serverName);

        if (match.Candidates.Count == 1)
        {
            var only = match.Candidates[0];
            return ((only.ServerId, only.ServerName), null);
        }

        if (match.Candidates.Count == 0)
        {
            return (default, Miss(servers));
        }

        return (default, McpHelpers.Refusal(
            "server_name",
            $"'{serverName.Trim()}' matches {match.Candidates.Count} monitored servers" +
            (match.MatchedBy == "exact" ? " (several registrations share that name)" : " (as part of their names)") +
            ". Pass one server's full name from this list:\n" + ListCandidates(match.Candidates)));
    }

    private static string Miss(IReadOnlyList<ServerConnection> servers) =>
        McpHelpers.Refusal("server_name", $"Could not resolve server. Available servers:\n{ListAvailableServers(servers)}");

    private static string ListCandidates(IReadOnlyList<ServerCandidate> candidates) =>
        string.Join("\n", candidates.Select(c =>
            string.IsNullOrEmpty(c.DisplayName) || c.DisplayName == c.ServerName
                ? $"{c.ServerName} [{c.Kind}]"
                : $"{c.ServerName} [{c.Kind}] ({c.DisplayName})"));

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
        return ResolveIn(serverManager.GetEnabledServers(), serverName);
    }

    /// <summary>
    /// One server a name answers to, for a write that must not guess which one the caller meant (#4734).
    /// <paramref name="ServerName"/> is the STORAGE name (<see cref="RemoteCollectorService.GetServerNameForStorage"/>:
    /// the database and <c>:RO</c> suffixes included), the identity <paramref name="ServerId"/> is hashed from and
    /// the name a caller can pass back to pick exactly this registration. <paramref name="Kind"/> says which of a
    /// machine's registrations it is (<see cref="PerformanceMonitor.Common.ServerIdHelper.DescribeKind"/>: plain,
    /// read-only, per-database), read from the database name and read-only intent the storage name was built from.
    /// </summary>
    internal sealed record ServerCandidate(int ServerId, string ServerName, string DisplayName, string Kind);

    /// <summary>
    /// What <see cref="MatchCandidates"/> found: every server the name answers to, and the rule that found them
    /// (<c>exact</c>, <c>partial</c>, or <c>none</c> when there are no candidates).
    /// </summary>
    internal sealed record CandidateMatch(IReadOnlyList<ServerCandidate> Candidates, string MatchedBy);

    /// <summary>
    /// Every enabled server a name answers to. The write tool (#4734: <c>mute_analysis_finding</c>) refuses an
    /// ambiguous name outright because it persists a row against whichever server the name landed on, and the read
    /// tools (<see cref="ResolveIn"/>) refuse it too: a first-match read answered for the wrong database when several
    /// databases on one host shared a host name. This is the
    /// same rule Darling's <c>mute_analysis_finding</c> applies: the ONE registration whose storage name equals the name
    /// exactly (case-sensitive), if there is one; otherwise every EXACT match (case-insensitive) on the server name, the
    /// display name or the storage name, if there is at least one; otherwise every PARTIAL match (<c>Contains</c>,
    /// case-insensitive) on the server name or display name. The storage name is an exact key so a candidate's own
    /// <c>server</c> value, as listed in an <c>ambiguous</c> answer, selects that registration when the caller passes it
    /// back.
    ///
    /// <para><b>Why the first tier is case-sensitive and stops at one (#4734).</b> The plain registration's storage
    /// name IS the machine name that its read-only and per-database siblings share as their <c>ServerName</c>, so
    /// without this tier the value the ambiguous answer lists for the plain registration tied with its own siblings
    /// and no name could pick it. A storage name is unique (the server_id is hashed from it), so an exact match names
    /// one registration by construction. The tier is narrow on purpose: the same name in another case, a display-name
    /// match and a partial match all fall through to the matching below and still answer ambiguous when they name
    /// several registrations.</para>
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

        var byStorageName = DistinctByStorageName(servers.Where(s =>
            string.Equals(RemoteCollectorService.GetServerNameForStorage(s), name, StringComparison.Ordinal)));
        if (byStorageName.Count == 1)
        {
            return new CandidateMatch(byStorageName, "exact");
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
                    server.DisplayName,
                    PerformanceMonitor.Common.ServerIdHelper.DescribeKind(server.DatabaseName, server.ReadOnlyIntent)));
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
