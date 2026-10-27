/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>get_resource_use_by_tag</c> (#5983, wave 2 of #5971): who is using what, rolled up by query tag. A STORED read over the
/// materialised tag map (<c>config.query_tag_map</c>): nothing evaluates a rule at read time. One section per source (plan cache,
/// Query Store, procedures, blocked-process reports, DMV blocking snapshots, deadlocks); the sections are never added together,
/// because the plan cache and Query Store count the same executions and procedures overlap both. Untagged work is its own row in
/// each section, and within a section the tag rows' own figures plus <c>untagged</c> add up to <c>total</c>. The SQL is
/// <see cref="QueryTagRollupSql"/>, shared with the viewer (#5988).
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpQueryTagRollupTools
{
    internal const int DefaultLimit = 25;
    private const int MaxLimit = 200;

    [McpServerTool(Name = "get_resource_use_by_tag"), Description("Rolls resource use up by query tag, one section per source: plan_cache, query_store, procedures, blocking_reports, blocking_snapshots, deadlocks. SECTIONS ARE NEVER SUMMED: the plan cache and Query Store count the same executions. Each section has tag rows (own figure and subtree figure), an untagged row and a total. Omit server_name for the fleet. tag limits the answer to that tag's subtree. <<GUIDE>> Rolls resource use up by query tag from the tag map (config.query_tag_map); nothing is evaluated at read time, so a rule change shows after the map's next pass. Sections: plan_cache (cpu_ms, duration_ms, logical_reads, logical_writes, executions), query_store (the same plus peak_memory_pages, a maximum not a sum; read only when hours_back is 24 or more), procedures (as plan_cache), blocking_reports and blocking_snapshots (blocked_ms is wait time on the blocked side's tag, caused_ms the same wait on the blocking side's tag; the snapshot figures repeat a still-open wait once per sample), deadlocks (deadlocks and processes: a deadlock counts once for each tag among its processes, so the rows do not add up to the total). Within a section the tag rows' own figures plus untagged add up to total; a subtree figure is a tag plus its children, a parent's own figure leaves its children out. A key tagged on one server is untagged on another. Fleet answers list each server and give fleet_total per section. With tag set there is no untagged row and total is that tag's subtree. truncated says a section held more tag rows than limit; scan_truncated says the deadlock graphs read hit their ceiling. A section status of empty means no rows in the window; unavailable means the source does not exist in this store yet or, for query_store, one of three things: its table is missing, the window is under the 24-hour minimum its interval table is read for (ask for hours_back 24 or more), or the interval table's gate refused a server (no coverage row, a pending batch, or a failed decision; the section names that server). The sections answer independently: the top-level status is unavailable only when every section is, empty when every served section is empty, and a top-level message names any section not served.")]
    public static Task<string> GetResourceUseByTag(
        NpgsqlDataSource postgres,
        [Description("Server name or display name. Omit for the whole fleet.")] string? server_name = null,
        [Description("Hours of history. Default 24. The query_store section is read only for 24 or more.")] int hours_back = 24,
        [Description("A tag name or id: shows only that tag's subtree (the tag and its children), with no untagged row.")] string? tag = null,
        [Description("Maximum tag rows per section (per server in the fleet view). Default 25. Read truncated to know whether a section held more.")] int limit = DefaultLimit,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
        => GetResourceUseByTagCore(postgres, server_name, hours_back, tag, limit, as_of, logger, cancellationToken);

    internal static async Task<string> GetResourceUseByTagCore(
        NpgsqlDataSource postgres, string? server_name, int hours_back, string? tag, int limit, string? as_of, ILogger? logger,
        CancellationToken cancellationToken)
    {
        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;
        if (limit > MaxLimit)
        {
            return McpHelpers.Refusal("limit", $"limit must be at most {MaxLimit}.");
        }

        try
        {
            var (servers, fault) = await DarlingServerResolver.LoadEnabledOrFaultAsync(postgres, cancellationToken);
            if (fault != null) return fault;

            var fleet = string.IsNullOrWhiteSpace(server_name);
            List<RegisteredServer> scope;
            if (fleet)
            {
                scope = servers.ToList();
                if (scope.Count == 0)
                {
                    return McpHelpers.Status("empty", "No servers are registered, so there is no resource use to roll up.");
                }
            }
            else
            {
                var (resolved, error) = DarlingServerResolver.ResolveOrError(servers, server_name);
                if (error != null) return error;
                scope = servers.Where(s => s.ServerId == resolved.ServerId).ToList();
            }

            var tags = await DarlingQueryTagRollupReader.ReadTagsAsync(postgres, cancellationToken, McpCommandDeadlines.ReadSeconds);
            int? tagFilter = null;
            if (!string.IsNullOrWhiteSpace(tag))
            {
                var (found, problem) = ResolveTag(tags, tag.Trim());
                if (problem != null) return McpHelpers.Refusal("tag", problem);
                tagFilter = found;
            }

            var windowStart = windowEnd.AddHours(-hours_back);
            var serverIds = scope.Select(s => s.ServerId).ToArray();
            var names = scope.ToDictionary(s => s.ServerId, s => s.ServerName);

            /* Each stats section routes on its own (their rollups and floors are independent) and says how far back it reached: the raw branch
               carries effective_start / window_truncated, the hourly branch the served window_end. The routing lives in the reader, which judges
               the retention tier against the wall clock; this tool's only "now" is the as_of anchor (windowEnd). A read for more than one server
               takes the hourly rollup also inside raw retention when it serves most of the window (#6794, QueryTagFleetRoute). */
            var (planRoute, procRoute) = await DarlingQueryTagRollupRouting.ChooseStatsRoutesAsync(postgres, serverIds, windowStart, windowEnd, cancellationToken);

            var sections = new List<object>();
            /* anyWork: any collected work in the window at all. anyData: the answer holds figures, so with a tag filter it is decided from the
               TAG-FILTERED facts (a known tag that matches nothing in a window that holds work answers empty, with its own message, not as
               zero-figure data; matches the Lite twin, #5992). */
            var anyWork = false;
            var anyData = false;
            /* Sections this call could not serve, with why. The top-level status is "unavailable" only when every section is in here (#5983). */
            var notServed = new List<(string Section, string Reason)>();

            async Task AddAdditive(QueryTagRollupSection section, string sql, string source, DateTime? ceiling, Dictionary<string, object?>? disclosure)
            {
                List<QueryTagRollupFact> facts;
                try
                {
                    facts = await DarlingQueryTagRollupReader.ReadFactsAsync(
                        postgres, sql, section, serverIds, windowStart, windowEnd, ceiling, cancellationToken, McpCommandDeadlines.ReadSeconds);
                }
                catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
                {
                    var reason = $"{source} does not exist in this store yet.";
                    notServed.Add((section.Name, reason));
                    sections.Add(Merge(Wrap(section, source, fleet, scope, names, tags, tagFilter, limit, [], "unavailable", reason, null), disclosure));
                    return;
                }

                anyWork |= facts.Count > 0;
                anyData |= HasMatch(facts, tags, tagFilter);
                sections.Add(Merge(Wrap(section, source, fleet, scope, names, tags, tagFilter, limit, facts, facts.Count == 0 ? "empty" : "ok",
                    facts.Count == 0 ? "No rows in the window." : null, null), disclosure));
            }

            await AddAdditive(
                QueryTagRollupSection.PlanCache,
                planRoute.Hourly ? QueryTagRollupSql.PlanCacheHourlySql : QueryTagRollupSql.PlanCacheRawSql,
                planRoute.Hourly ? "collect.query_stats_io_hourly (hourly rollup)" : "collect.query_stats",
                planRoute.Ceiling, planRoute.Disclosure);

            /* Query Store reads the composed interval table only, and only where the gate every other reader of it applies lets it. There is
               no raw fallback: raw query_store_stats re-collects an open interval every pass, so summing raw counts one interval many times
               (get_query_store_top dedupes raw; a roll-up that sums cannot). A server the gate refuses is "unavailable" with the reason, not
               "empty" (#3553): its Query Store work may be sitting in raw. The gate and the read share one snapshot (the reader). A section that
               ANY server refuses is not fully served, so it is named in notServed even when other servers were read (a fleet answer of
               "empty" must not hide the refusal). */
            {
                var read = await DarlingQueryTagRollupReader.ReadQueryStoreAsync(postgres, serverIds, windowStart, windowEnd, logger, cancellationToken, McpCommandDeadlines.ReadSeconds);
                var allowed = read.Allowed;
                var refused = read.Refused;
                var facts = read.Facts;
                var unavailable = read.TableMissing;
                if (unavailable)
                {
                    facts.Clear();
                }

                var reach = new Dictionary<string, object?>();
                if (allowed.Count > 0)
                {
                    var from = allowed.Values.Max();
                    reach["effective_start"] = McpHelpers.FormatEffectiveStart(from);
                    reach["window_truncated"] = RawWindowFloor.IsTruncated(from, windowStart);
                }

                anyWork |= facts.Count > 0;
                anyData |= HasMatch(facts, tags, tagFilter);
                var refusalNote = refused.Count == 0
                    ? null
                    : "Query Store is read from the composed interval table only, and it is not readable for " + string.Join("; ",
                        refused.OrderBy(r => r.Key).Select(r => $"{names[r.Key]} ({r.Value})")) + ".";
                if (unavailable || allowed.Count == 0)
                {
                    var reason = unavailable
                        ? "collect.query_store_interval_wide does not exist in this store yet."
                        : fleet
                            ? refusalNote!
                            : "Query Store is read from the composed interval table only, and it is not readable here: " + refused.Values.First() + ".";
                    notServed.Add((QueryTagRollupSection.QueryStore.Name, reason));
                    sections.Add(Merge(Wrap(QueryTagRollupSection.QueryStore, "collect.query_store_interval_wide", fleet, scope, names, tags, tagFilter, limit,
                        [], "unavailable", reason, refused), reach));
                }
                else
                {
                    if (refusalNote is not null)
                    {
                        notServed.Add((QueryTagRollupSection.QueryStore.Name, refusalNote));
                    }

                    var message = facts.Count == 0
                        ? (refusalNote is null ? "No rows in the window." : "No rows in the window for the servers that were read. " + refusalNote)
                        : refusalNote;
                    sections.Add(Merge(Wrap(QueryTagRollupSection.QueryStore, "collect.query_store_interval_wide", fleet, scope, names, tags, tagFilter, limit,
                        facts, facts.Count == 0 ? "empty" : "ok", message, refused), reach));
                }
            }

            await AddAdditive(
                QueryTagRollupSection.Procedures,
                procRoute.Hourly ? QueryTagRollupSql.ProceduresHourlySql : QueryTagRollupSql.ProceduresRawSql,
                procRoute.Hourly ? "collect.procedure_stats_io_hourly (hourly rollup)" : "collect.procedure_stats",
                procRoute.Ceiling, procRoute.Disclosure);
            await AddAdditive(QueryTagRollupSection.BlockingReports, QueryTagRollupSql.BlockingReportsSql, "collect.blocked_process_reports", null, null);
            await AddAdditive(QueryTagRollupSection.BlockingSnapshots, QueryTagRollupSql.BlockingSnapshotsSql, "collect.dmv_blocking_snapshots", null, null);

            try
            {
                var (deadlocks, scanTruncated) = await DarlingQueryTagRollupReader.ReadDeadlocksAsync(postgres, serverIds, windowStart, windowEnd, logger, cancellationToken, McpCommandDeadlines.ReadSeconds);
                anyWork |= deadlocks.Count > 0;
                anyData |= HasMatchDeadlocks(deadlocks, tags, tagFilter);
                sections.Add(WrapDeadlocks(fleet, scope, names, tags, tagFilter, limit, deadlocks, scanTruncated));
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                const string reason = "collect.deadlocks does not exist in this store yet.";
                notServed.Add((QueryTagRollupSection.Deadlocks.Name, reason));
                sections.Add(Wrap(QueryTagRollupSection.Deadlocks, "collect.deadlocks", fleet, scope, names, tags, tagFilter, limit, [], "unavailable", reason, null));
            }

            if (!anyWork)
            {
                if (!fleet)
                {
                    var unmet = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, scope[0].ServerId, scope[0].ServerName, "query_stats", cancellationToken);
                    if (unmet != null) return unmet;
                }

                /* Six independent sections answer from the ones they could serve: "unavailable" only when none could be, else "empty" because
                   every served section held no rows. The sections that were not served are named, with why. */
                if (notServed.Count == sections.Count)
                {
                    return McpHelpers.Status("unavailable", "No section of the roll-up could be read. " + NotServedText(notServed));
                }
            }

            /* Data, or an empty answer: no served section held figures (nothing collected, or nothing carrying the requested tag), so the status
               is "empty", and the sections ride along (Lite does the same) so each section's own status and any unserved section's reason are
               there to read. */
            var payload = new Dictionary<string, object?>();
            if (!anyData)
            {
                payload["status"] = "empty";
            }

            payload["scope"] = fleet ? "fleet" : "server";
            payload["server"] = fleet ? null : scope[0].ServerName;
            payload["hours_back"] = hours_back;
            payload["window_start"] = windowStart.ToString("o", CultureInfo.InvariantCulture);
            payload["window_end"] = windowEnd.ToString("o", CultureInfo.InvariantCulture);
            payload["tag"] = tagFilter is int id ? tags.First(t => t.Id == id).Name : null;
            /* The sections never add up: the plan cache and Query Store count the same executions and procedures overlap both. */
            payload["sections_additive"] = false;
            if (!anyData)
            {
                var emptyMessage = !anyWork
                    ? "No resource use was collected in the window" + (fleet ? " on any server." : " on this server.")
                    : $"No collected work carries query tag '{tags.First(t => t.Id == tagFilter!.Value).Name}' in the window" + (fleet ? " on any server." : " on this server.");
                payload["message"] = notServed.Count > 0 ? emptyMessage + " Sections not served: " + NotServedText(notServed) : emptyMessage;
            }
            else if (notServed.Count > 0)
            {
                payload["message"] = "Sections not served: " + NotServedText(notServed);
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

    /// <summary>Whether the facts hold figures for the request: any fact at all without a tag filter, else a fact on the tag or a tag under it.</summary>
    private static bool HasMatch(List<QueryTagRollupFact> facts, IReadOnlyList<QueryTagNode> tags, int? tagFilter)
    {
        if (tagFilter is not int filter)
        {
            return facts.Count > 0;
        }

        var byId = tags.ToDictionary(t => t.Id);
        return facts.Any(f => f.TagId is int id && byId.ContainsKey(id) && DarlingQueryTagRollupReader.IsUnder(id, filter, byId));
    }

    /// <summary>The deadlock twin of <see cref="HasMatch"/>: a graph counts where one of its processes carries the tag or a tag under it.</summary>
    private static bool HasMatchDeadlocks(List<QueryTagDeadlockFact> facts, IReadOnlyList<QueryTagNode> tags, int? tagFilter)
    {
        if (tagFilter is not int filter)
        {
            return facts.Count > 0;
        }

        var byId = tags.ToDictionary(t => t.Id);
        return facts.Any(f => f.ProcessesByTag.Any(p => p.Value > 0 && byId.ContainsKey(p.Key) && DarlingQueryTagRollupReader.IsUnder(p.Key, filter, byId)));
    }

    private static string NotServedText(List<(string Section, string Reason)> notServed)
        => string.Join(" ", notServed.Select(n => $"{n.Section} is unavailable: {n.Reason}"));

    private static Dictionary<string, object?> Merge(Dictionary<string, object?> section, Dictionary<string, object?>? extra)
    {
        if (extra is not null)
        {
            foreach (var (key, value) in extra)
            {
                section[key] = value;
            }
        }

        return section;
    }

    private static (int? Id, string? Problem) ResolveTag(IReadOnlyList<QueryTagNode> tags, string text)
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

    private static Dictionary<string, object?> Wrap(
        QueryTagRollupSection section, string source, bool fleet, IReadOnlyList<RegisteredServer> scope,
        Dictionary<int, string> names, IReadOnlyList<QueryTagNode> tags, int? tagFilter, int limit,
        List<QueryTagRollupFact> facts, string status, string? message, Dictionary<int, string>? refused)
    {
        Dictionary<string, object?> block;
        if (!fleet)
        {
            block = facts.Count == 0
                ? DarlingQueryTagRollupReader.Empty(section, status, message ?? string.Empty, tagFilter)
                : DarlingQueryTagRollupReader.Assemble(section, facts, tags, tagFilter, limit);
            block["source"] = source;
            return block;
        }

        var servers = scope.Select(s =>
        {
            var mine = facts.Where(f => f.ServerId == s.ServerId).ToList();
            var serverBlock = refused is not null && refused.TryGetValue(s.ServerId, out var why)
                ? DarlingQueryTagRollupReader.Empty(section, "unavailable", "Not readable for this server: " + why + ".", tagFilter)
                : mine.Count == 0
                    ? DarlingQueryTagRollupReader.Empty(section, status == "ok" ? "empty" : status, message ?? "No rows in the window for this server.", tagFilter)
                    : DarlingQueryTagRollupReader.Assemble(section, mine, tags, tagFilter, limit);
            serverBlock["server"] = names[s.ServerId];
            return (object)serverBlock;
        }).ToList();
        return new Dictionary<string, object?>
        {
            ["section"] = section.Name,
            ["source"] = source,
            ["status"] = status,
            ["message"] = message,
            ["servers"] = servers,
            ["fleet_total"] = DarlingQueryTagRollupReader.FleetTotal(section, facts, tags, tagFilter),
        };
    }

    private static Dictionary<string, object?> WrapDeadlocks(
        bool fleet, IReadOnlyList<RegisteredServer> scope, Dictionary<int, string> names,
        IReadOnlyList<QueryTagNode> tags, int? tagFilter, int limit, List<QueryTagDeadlockFact> facts, bool scanTruncated)
    {
        var section = QueryTagRollupSection.Deadlocks;
        var status = facts.Count == 0 ? "empty" : "ok";
        var message = facts.Count == 0 ? "No deadlock graphs in the window." : null;
        Dictionary<string, object?> One(IReadOnlyList<QueryTagDeadlockFact> mine)
        {
            if (mine.Count > 0)
            {
                return DarlingQueryTagRollupReader.AssembleDeadlocks(mine, tags, tagFilter, limit);
            }

            var empty = DarlingQueryTagRollupReader.Empty(section, "empty", "No deadlock graphs in the window.", tagFilter);
            empty["unparsed_graphs"] = 0;
            return empty;
        }

        Dictionary<string, object?> block;
        if (!fleet)
        {
            block = One(facts.ToList());
            block["source"] = "collect.deadlocks";
            block["scan_truncated"] = scanTruncated;
            return block;
        }

        var servers = scope.Select(s =>
        {
            var serverBlock = One(facts.Where(f => f.ServerId == s.ServerId).ToList());
            serverBlock["server"] = names[s.ServerId];
            return (object)serverBlock;
        }).ToList();
        return new Dictionary<string, object?>
        {
            ["section"] = section.Name,
            ["source"] = "collect.deadlocks",
            ["status"] = status,
            ["message"] = message,
            ["scan_truncated"] = scanTruncated,
            ["unparsed_graphs"] = facts.Count(f => f.Unparsed),
            ["rows_add_up_to_total"] = false,
            ["note"] = "A deadlock is counted once for each tag among its processes, so the deadlock rows do not add up to the total.",
            ["servers"] = servers,
            ["fleet_total"] = DarlingQueryTagRollupReader.FleetDeadlockTotal(facts, tags, tagFilter),
        };
    }
}
