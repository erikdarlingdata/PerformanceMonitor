using System.ComponentModel;
using System.Globalization;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Collectors;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// <c>get_resource_use_by_tag</c> (#5992): who is using what, rolled up by query tag. The Lite twin of Darling's tool (#5983), with the
/// same name, parameters, sections and output shape. A STORED read over the materialised tag map (<c>query_tag_map</c>): nothing
/// evaluates a rule at read time. Thin over <see cref="LocalDataService.GetQueryTagRollupDataAsync"/> and <see cref="QueryTagRollup"/>,
/// the readers the Lite UI (#5993) calls too. Omit server_name for the all-servers view.
/// </summary>
[McpServerToolType]
public sealed class McpQueryTagUsageTools
{
    private const int DefaultLimit = 25;
    private const int MaxLimit = 200;

    [McpServerTool(Name = "get_resource_use_by_tag"), Description("Rolls resource use up by query tag, one section per source: plan_cache, query_store, procedures, blocking_reports, blocking_snapshots, deadlocks. SECTIONS ARE NEVER SUMMED: the plan cache and Query Store count the same executions. Each section has tag rows (own figure and subtree figure), an untagged row and a total. Omit server_name for the fleet. tag limits the answer to that tag's subtree. <<GUIDE>> Rolls resource use up by query tag from the tag map (query_tag_map); nothing is evaluated at read time, so a rule change shows after the map's next pass. Sections: plan_cache (cpu_ms, duration_ms, logical_reads, logical_writes, executions), query_store (the same plus peak_memory_pages, a maximum not a sum), procedures (as plan_cache), blocking_reports and blocking_snapshots (blocked_ms is wait time on the blocked side's tag, caused_ms the same wait on the blocking side's tag; the snapshot figures repeat a still-open wait once per sample), deadlocks (deadlocks and processes: a deadlock counts once for each tag among its processes, so the rows do not add up to the total). Within a section the tag rows' own figures plus untagged add up to total; a subtree figure is a tag plus its children, a parent's own figure leaves its children out. A key tagged on one server is untagged on another. The all-servers answer lists each server and gives fleet_total per section. With tag set there is no untagged row and total is that tag's subtree. truncated says a section held more tag rows than limit; scan_truncated says the deadlock graphs read hit their ceiling. A section status of empty means no rows in the window; unavailable means the section's source table does not exist in this store. A tag that exists but matches no row in the window gives a top-level empty with a message saying so. The sections answer independently: the top-level status is unavailable only when every section is, empty when every served section is empty, and a top-level message names any section not served.")]
    public static async Task<string> GetResourceUseByTag(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name. Omit for all servers.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("A tag name or id: shows only that tag's subtree (the tag and its children), with no untagged row.")] string? tag = null,
        [Description("Maximum tag rows per section (per server in the all-servers view). Default 25. Read truncated to know whether a section held more.")] int limit = DefaultLimit,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var answer = await GetResourceUseByTagCore(dataService, serverManager, server_name, hours_back, tag, limit, as_of);
        /* #7005: the window is fully covered only when every table the roll-up reads is, so the floor is the LATEST of the six sections' floors
           (each the earliest any scoped server answers), and the one notice sits at the top level, not per section. */
        if (McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) != null)
        {
            return answer;
        }

        try
        {
            List<int> ids;
            if (string.IsNullOrWhiteSpace(server_name))
            {
                ids = serverManager.GetEnabledServers().Select(RemoteCollectorService.GetServerId).ToList();
            }
            else
            {
                var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
                if (error != null) return answer;
                ids = [resolved.ServerId];
            }

            if (ids.Count == 0)
            {
                return answer;
            }

            var windowStart = windowEnd.AddHours(-hours_back);
            /* One fleet-form probe per section relation, scoped to the servers the roll-up reads (six at most, whatever the server count), not one per relation per server. */
            return await McpQueryTools.WithProbedWindowNoticeAsync(
                answer,
                () => LatestSectionFloorAsync(relation => dataService.GetFleetWindowFloorAsync(relation, windowStart, windowEnd, ids)),
                () => LatestSectionFloorAsync(relation => dataService.GetOldestFleetWindowRowAsync(relation, ids)),
                windowStart, windowEnd, "query tag roll-up tables");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_resource_use_by_tag", ex);
        }
    }

    /// <summary>The six relations the roll-up reads, one per section (plan_cache, query_store, procedures, blocking_reports, blocking_snapshots, deadlocks).</summary>
    private static readonly QueryWindowRelation[] SectionRelations =
    [
        QueryWindowRelation.QueryStats, QueryWindowRelation.QueryStoreStats, QueryWindowRelation.ProcedureStats,
        QueryWindowRelation.BlockedProcessReports, QueryWindowRelation.DmvBlockingSnapshots, QueryWindowRelation.Deadlocks,
    ];

    /// <summary>The LATEST of the sections' answers: a section that answers null (nothing in the window) gives way to the others.</summary>
    private static async Task<DateTime?> LatestSectionFloorAsync(Func<QueryWindowRelation, Task<DateTime?>> perRelation)
    {
        DateTime? latest = null;
        foreach (var relation in SectionRelations)
        {
            /* A section whose table is missing is already answered "unavailable" by the roll-up itself; it has no floor, so it gives way to the others. */
            DateTime? floor;
            try
            {
                floor = await perRelation(relation);
            }
            catch (DuckDB.NET.Data.DuckDBException)
            {
                floor = null;
            }

            latest = LocalDataService.LaterCoverageFloor(latest, floor);
        }

        return latest;
    }

    private static async Task<string> GetResourceUseByTagCore(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name. Omit for all servers.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("A tag name or id: shows only that tag's subtree (the tag and its children), with no untagged row.")] string? tag = null,
        [Description("Maximum tag rows per section (per server in the all-servers view). Default 25. Read truncated to know whether a section held more.")] int limit = DefaultLimit,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var windowError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (windowError != null) return windowError;
        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return limitError;
        if (limit > MaxLimit)
        {
            return McpHelpers.Refusal("limit", $"limit must be at most {MaxLimit}.");
        }

        try
        {
            var allServers = string.IsNullOrWhiteSpace(server_name);
            List<(int Id, string Name)> scope;
            if (allServers)
            {
                /* #6860: every name in the answer is the display name (#6179), the one the caller passes back as server_name, not the storage key. */
                var enabled = serverManager.GetEnabledServers();
                scope = enabled
                    .Select(s => (RemoteCollectorService.GetServerId(s), ServerResolver.AnswerNameOf(enabled, s, RemoteCollectorService.GetServerNameForStorage(s)))).ToList();
                if (scope.Count == 0)
                {
                    return McpHelpers.Status("empty", "No servers are registered, so there is no resource use to roll up.");
                }
            }
            else
            {
                var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
                if (error != null) return error;
                scope = [(resolved.ServerId, resolved.DisplayName)];
            }

            var windowStart = windowEnd.AddHours(-hours_back);
            var data = await dataService.GetQueryTagRollupDataAsync(scope.Select(s => s.Id).ToList(), windowStart, windowEnd);

            int? tagFilter = null;
            if (!string.IsNullOrWhiteSpace(tag))
            {
                var (found, problem) = ResolveTag(data.Tags, tag.Trim());
                if (problem != null) return McpHelpers.Refusal("tag", problem);
                tagFilter = found;
            }

            var sections = new List<object>();
            /* With a tag set, "data" means data under that tag: a tag that exists but matches nothing is an empty answer with its own
               message, not data with every section ok and zero rows (#6023 review). anyUnfiltered says the window held rows at all. */
            var anyUnfiltered = data.Deadlocks.Count > 0;
            var anyData = tagFilter is null
                ? data.Deadlocks.Count > 0
                : data.Deadlocks.Any(d => d.ProcessesByTag.Keys.Any(k => QueryTagRollup.IsUnderTag(k, data.Tags, tagFilter)));
            var notServed = new List<(string Section, string Reason)>();
            foreach (var section in new[]
            {
                QueryTagRollupSection.PlanCache, QueryTagRollupSection.QueryStore, QueryTagRollupSection.Procedures,
                QueryTagRollupSection.BlockingReports, QueryTagRollupSection.BlockingSnapshots,
            })
            {
                var facts = data.Sections[section.Name];
                anyUnfiltered |= facts.Count > 0;
                anyData |= facts.Any(f => QueryTagRollup.IsUnderTag(f.TagId, data.Tags, tagFilter));
                data.Unavailable.TryGetValue(section.Name, out var why);
                if (why is not null) notServed.Add((section.Name, why));
                sections.Add(QueryTagRollup.Section(section, facts, scope, allServers, data.Tags, tagFilter, limit, why));
            }

            data.Unavailable.TryGetValue(QueryTagRollupSection.Deadlocks.Name, out var deadlockWhy);
            if (deadlockWhy is not null) notServed.Add((QueryTagRollupSection.Deadlocks.Name, deadlockWhy));
            sections.Add(QueryTagRollup.DeadlockSection(data.Deadlocks, scope, allServers, data.Tags, tagFilter, limit, data.ScanTruncated, deadlockWhy));

            /* Six independent sections answer from the ones they could serve: "unavailable" only when none could be, else "empty" when the
               served ones held no rows, else data. The sections not served are named with why. Lite has no interval tier and no
               Query Store coverage gate, and none of the six source collectors is gated off on any SQL Server edition (so no per-section not_collected, #6023), so a missing table is the one way a section is unavailable here. */
            if (!anyUnfiltered)
            {
                /* The same one top-level engine question Darling's twin asks (query_stats, one server, only when the window holds no work at
                   all), so both SKUs wire this read to the same collector (EngineCapabilityReadWiringTests). On every SQL Server edition
                   Lite collects today it answers null and the empty answer below stands. */
                if (!allServers)
                {
                    var unmet = await McpEngineCapability.NotCollectedStatusAsync(dataService, scope[0].Id, scope[0].Name, "query_stats", awsRdsKind: AwsRdsKind.None);
                    if (unmet != null) return unmet;
                }

                if (notServed.Count == sections.Count)
                {
                    return McpHelpers.Status("unavailable", "No section of the roll-up could be read. " + NotServedText(notServed));
                }
            }

            var payload = new Dictionary<string, object?>();
            if (!anyData)
            {
                payload["status"] = "empty";
            }

            payload["scope"] = allServers ? "fleet" : "server";
            payload["server"] = allServers ? null : scope[0].Name;
            payload["hours_back"] = hours_back;
            payload["window_start"] = windowStart.ToString("o", CultureInfo.InvariantCulture);
            payload["window_end"] = windowEnd.ToString("o", CultureInfo.InvariantCulture);
            payload["tag"] = tagFilter is int id ? data.Tags.First(t => t.Id == id).Name : null;
            payload["sections_additive"] = false;
            var tagMissed = tagFilter is not null && !anyData && anyUnfiltered;
            var tagMissText = tagMissed
                ? $"The window held resource use, but none of it is under query tag '{data.Tags.First(t => t.Id == tagFilter).Name}', so every section is empty for that tag. Drop tag to see what the window holds. "
                : string.Empty;
            if (notServed.Count > 0)
            {
                payload["message"] = tagMissText + (anyData || tagMissed ? string.Empty : "No resource use was collected in the window" + (allServers ? " on any server. " : " on this server. "))
                    + "Sections not served: " + NotServedText(notServed);
            }
            else if (tagMissed)
            {
                payload["message"] = tagMissText.TrimEnd();
            }
            else if (!anyData)
            {
                payload["message"] = "No resource use was collected in the window" + (allServers ? " on any server." : " on this server.");
            }

            payload["note"] = "Each section names its own source and is never summed with another. Within a section the tag rows' own figures plus untagged add up to total; a subtree figure is a tag plus its children.";
            payload["sections"] = sections;
            return JsonSerializer.Serialize(payload, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_resource_use_by_tag", ex);
        }
    }

    private static string NotServedText(List<(string Section, string Reason)> notServed)
        => string.Join(" ", notServed.Select(n => $"{n.Section} is unavailable: {n.Reason}"));

    private static (int? Id, string? Problem) ResolveTag(IReadOnlyList<QueryTagRow> tags, string text)
    {
        if (int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var id) && tags.Any(t => t.Id == id))
        {
            return (id, null);
        }

        var matches = tags.Where(t => string.Equals(t.Name, text, StringComparison.OrdinalIgnoreCase)).ToList();
        return matches.Count switch
        {
            1 => (matches[0].Id, null),
            0 => (null, $"No query tag is named or numbered '{text}'."),
            _ => (null, $"{matches.Count} query tags are named '{text}'; pass the tag id instead ({string.Join(", ", matches.Select(m => m.Id))})."),
        };
    }
}
