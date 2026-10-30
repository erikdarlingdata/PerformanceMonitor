using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpDiscoveryTools
{
    [McpServerTool(Name = "find_database"), Description("Finds which monitored server holds a database, by exact name (case-insensitive), from each server's newest database list. Tickets and traces name the tenant database, not the server. One row per matching server: registry name, display name, is_monitored (false = disabled), newest_snapshot_at (when the store last saw that list) and snapshot_current (false = older than 6 hours, so the match may be stale). empty = no newest list names it; a dropped, renamed or not-yet-collected database looks the same. Servers with no list in 7 days are not searched. not_collected = every server is an Azure SQL Database, which writes no list.")]
    public static async Task<string> FindDatabase(
        ServerManager serverManager,
        LocalDataService dataService,
        [Description("Database name to find. Exact match, case-insensitive.")] string database_name)
    {
        var requested = (database_name ?? string.Empty).Trim();
        if (requested.Length == 0)
        {
            return McpHelpers.Refusal("database_name", "database_name is required: the name of the database to find.");
        }

        try
        {
            var now = DateTime.UtcNow;
            var lists = await dataService.FindDatabaseAsync(requested, now);
            var servers = serverManager.GetAllServers()
                .Select(s => (Server: s, Id: RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s))))
                .ToList();
            var searched = lists.Where(l => servers.Any(s => s.Id == l.ServerId)).ToList();
            var matches = searched.Where(l => l.MatchedName is not null).ToList();
            var days = (int)LocalDataService.FindDatabaseLookBack.TotalDays;
            if (matches.Count == 0)
            {
                /* #6178: no server has a list. When every server runs an engine that never writes one (Azure SQL Database), that is
                   not_collected, not a true negative. One server that could have had the list keeps the empty answer. */
                var fleet = servers.Select(s => (s.Id, s.Server.ServerName)).ToList();
                var fleetNotCollected = searched.Count == 0
                    ? await McpEngineCapability.FleetNotCollectedStatusAsync(dataService, fleet, "database_states")
                    : null;
                return fleetNotCollected ?? McpHelpers.Status(
                    "empty",
                    searched.Count == 0
                        ? $"No server has a database list in the last {days} days, so nothing was searched for '{requested}'. The database_states collector writes the list every minute by default (every 5 minutes under the Low-Impact preset) on a SQL Server target (not on Azure SQL Database), and only while Lite is open."
                        : $"No monitored server's newest database list names '{requested}' (searched {searched.Count} server(s) that have a list in the last {days} days). A database that was dropped, renamed, or created since the last list looks the same; servers with no list in that time (Azure SQL Database targets, or a stopped collector) were not searched.");
            }

            return JsonSerializer.Serialize(new
            {
                database_name = requested,
                servers_searched = searched.Count,
                matches = matches.Select(m =>
                {
                    var server = servers.First(s => s.Id == m.ServerId).Server;
                    return new
                    {
                        server_id = m.ServerId,
                        server_name = server.ServerName,
                        display_name = string.IsNullOrWhiteSpace(server.DisplayName) ? null : server.DisplayName,
                        database_name = m.MatchedName,
                        is_monitored = server.IsEnabled,
                        newest_snapshot_at = m.NewestSnapshotUtc.ToString("o"),
                        snapshot_current = now - m.NewestSnapshotUtc <= TimeSpan.FromHours(6),
                    };
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("find_database", ex);
        }
    }

    [McpServerTool(Name = "list_servers"), Description("Lists monitored servers with collection freshness status and last collection time. Darling has no live connection to monitored servers: status is derived from how recently each was collected (Online = fresh, Warning = stale, Offline = no recent collection). Lite's status IS a live connection check, independent of whether anything is being collected. <<GUIDE>> Use this first to see available servers before calling other tools. Lists all monitored SQL Server instances with their current status and last collection time.")]
    public static async Task<string> ListServers(ServerManager serverManager, LocalDataService dataService)
    {
        var servers = serverManager.GetEnabledServers();
        if (servers.Count == 0)
        {
            /* #6277: the `empty` status envelope, the same true negative Darling's list_servers answers an empty
               registry with, rather than bare prose a JSON client cannot tell from a refusal. */
            return McpHelpers.Status("empty", "No servers are configured.");
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
            var summary = await dataService.GetServerSummaryAsync(serverId, s.DisplayNameWithIntent, s.RegisteredAtUtc);
            /* #3967: a server whose history has aged out of the archive has no collection to show either,
               and "No data collected" would claim it never collected. */
            var lastCollection = summary?.LastCollectionTime?.ToString("o")
                ?? (summary?.CollectionFreshness == ServerFreshness.Offline ? "none retained" : "No data collected");

            lines.Add($"- {display} [{statusText}] (last collection: {lastCollection})");
        }

        return string.Join("\n", lines);
    }
}
