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
    private const int DefaultLimit = 25;
    private const int MaxLimit = 200;

    [McpServerTool(Name = "get_resource_use_by_tag"), Description("Rolls resource use up by query tag, one section per source: plan_cache, query_store, procedures, blocking_reports, blocking_snapshots, deadlocks. SECTIONS ARE NEVER SUMMED: the plan cache and Query Store count the same executions. Each section has tag rows (own figure and subtree figure), an untagged row and a total. Omit server_name for the fleet. tag limits the answer to that tag's subtree. <<GUIDE>> Rolls resource use up by query tag from the tag map (config.query_tag_map); nothing is evaluated at read time, so a rule change shows after the map's next pass. Sections: plan_cache (cpu_ms, duration_ms, logical_reads, logical_writes, executions), query_store (the same plus peak_memory_pages, a maximum not a sum), procedures (as plan_cache), blocking_reports and blocking_snapshots (blocked_ms is wait time on the blocked side's tag, caused_ms the same wait on the blocking side's tag; the snapshot figures repeat a still-open wait once per sample), deadlocks (deadlocks and processes: a deadlock counts once for each tag among its processes, so the rows do not add up to the total). Within a section the tag rows' own figures plus untagged add up to total; a subtree figure is a tag plus its children, a parent's own figure leaves its children out. A key tagged on one server is untagged on another. Fleet answers list each server and give fleet_total per section. With tag set there is no untagged row and total is that tag's subtree. truncated says a section held more tag rows than limit; scan_truncated says the deadlock graphs read hit their ceiling. A section status of empty means no rows in the window; unavailable means the source does not exist in this store yet.")]
    public static Task<string> GetResourceUseByTag(
        NpgsqlDataSource postgres,
        [Description("Server name or display name. Omit for the whole fleet.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
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
            List<DarlingServerResolver.RegisteredServer> scope;
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

            var tags = await DarlingQueryTagRollupReader.ReadTagsAsync(postgres, cancellationToken);
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

            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);
            var planHourly = UseHourly(
                DarlingDataReader.ChooseQueriesHourlyRoute(rollups, coverage, TopRanking.Reads, windowStart),
                RetentionTierRouter.Resolve(DateTime.UtcNow, windowStart, rollups.QueryGrainHourly, dailyAvailable: false,
                    coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView)));
            var procHourly = UseHourly(
                DarlingDataReader.ChooseProceduresHourlyRoute(rollups, coverage, TopRanking.Reads, windowStart),
                RetentionTierRouter.Resolve(DateTime.UtcNow, windowStart, rollups.ProcedureGrainHourly, dailyAvailable: false,
                    coverage.For(TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsDailyView)));
            var planCeiling = planHourly ? coverage.HourlyEndCeiling(TimescaleSupport.QueryStatsIoHourlyView, windowStart) : null;
            var procCeiling = procHourly ? coverage.HourlyEndCeiling(TimescaleSupport.ProcedureStatsIoHourlyView, windowStart) : null;

            var sections = new List<object>();
            var anyData = false;
            var anyUnavailable = false;

            async Task AddAdditive(QueryTagRollupSection section, string sql, string source, DateTime? ceiling)
            {
                List<QueryTagRollupFact> facts;
                try
                {
                    facts = await DarlingQueryTagRollupReader.ReadFactsAsync(
                        postgres, sql, section.Measures.Count, serverIds, windowStart, windowEnd, ceiling, cancellationToken);
                }
                catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
                {
                    anyUnavailable = true;
                    sections.Add(Wrap(section, source, fleet, scope, names, tags, tagFilter, limit, [], "unavailable",
                        $"{source} does not exist in this store yet."));
                    return;
                }

                anyData |= facts.Count > 0;
                sections.Add(Wrap(section, source, fleet, scope, names, tags, tagFilter, limit, facts, facts.Count == 0 ? "empty" : "ok",
                    facts.Count == 0 ? "No rows in the window." : null));
            }

            await AddAdditive(
                QueryTagRollupSection.PlanCache,
                planHourly ? QueryTagRollupSql.PlanCacheHourlySql : QueryTagRollupSql.PlanCacheRawSql,
                planHourly ? "collect.query_stats_io_hourly (hourly rollup)" : "collect.query_stats",
                planCeiling);
            await AddAdditive(QueryTagRollupSection.QueryStore, QueryTagRollupSql.QueryStoreSql, "collect.query_store_interval_wide", null);
            await AddAdditive(
                QueryTagRollupSection.Procedures,
                procHourly ? QueryTagRollupSql.ProceduresHourlySql : QueryTagRollupSql.ProceduresRawSql,
                procHourly ? "collect.procedure_stats_io_hourly (hourly rollup)" : "collect.procedure_stats",
                procCeiling);
            await AddAdditive(QueryTagRollupSection.BlockingReports, QueryTagRollupSql.BlockingReportsSql, "collect.blocked_process_reports", null);
            await AddAdditive(QueryTagRollupSection.BlockingSnapshots, QueryTagRollupSql.BlockingSnapshotsSql, "collect.dmv_blocking_snapshots", null);

            var (deadlocks, scanTruncated) = await DarlingQueryTagRollupReader.ReadDeadlocksAsync(postgres, serverIds, windowStart, windowEnd, cancellationToken);
            anyData |= deadlocks.Count > 0;
            sections.Add(WrapDeadlocks(fleet, scope, names, tags, tagFilter, limit, deadlocks, scanTruncated));

            if (!anyData)
            {
                if (!fleet)
                {
                    var unmet = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, scope[0].ServerId, scope[0].ServerName, "query_stats", cancellationToken);
                    if (unmet != null) return unmet;
                }

                return anyUnavailable
                    ? McpHelpers.Status("unavailable", "Some sources of the roll-up do not exist in this store yet, and the rest hold no rows in the window.")
                    : McpHelpers.Status("empty", "No resource use was collected in the window" + (fleet ? " on any server." : " on this server."));
            }

            return JsonSerializer.Serialize(new
            {
                scope = fleet ? "fleet" : "server",
                server = fleet ? null : scope[0].ServerName,
                hours_back,
                window_start = windowStart.ToString("o", CultureInfo.InvariantCulture),
                window_end = windowEnd.ToString("o", CultureInfo.InvariantCulture),
                tag = tagFilter is int id ? tags.First(t => t.Id == id).Name : null,
                /* The sections never add up: the plan cache and Query Store count the same executions and procedures overlap both. */
                sections_additive = false,
                note = "Each section names its own source and is never summed with another. Within a section the tag rows' own figures plus untagged add up to total; a subtree figure is a tag plus its children.",
                sections,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_resource_use_by_tag", ex);
        }
    }

    /// <summary>The hourly rollup serves a section only when the retention tier left raw and the io route reaches the window's start.</summary>
    private static bool UseHourly(HourlyRoute route, RetentionTier tier) =>
        tier != RetentionTier.Raw && route == HourlyRoute.Io;

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
        QueryTagRollupSection section, string source, bool fleet, IReadOnlyList<DarlingServerResolver.RegisteredServer> scope,
        Dictionary<int, string> names, IReadOnlyList<QueryTagNode> tags, int? tagFilter, int limit,
        List<QueryTagRollupFact> facts, string status, string? message)
    {
        Dictionary<string, object?> block;
        if (!fleet)
        {
            block = facts.Count == 0
                ? DarlingQueryTagRollupReader.Empty(section, status, message ?? string.Empty)
                : DarlingQueryTagRollupReader.Assemble(section, facts, tags, tagFilter, limit);
            block["source"] = source;
            return block;
        }

        var servers = scope.Select(s =>
        {
            var mine = facts.Where(f => f.ServerId == s.ServerId).ToList();
            var serverBlock = mine.Count == 0
                ? DarlingQueryTagRollupReader.Empty(section, status == "ok" ? "empty" : status, message ?? "No rows in the window for this server.")
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
            ["fleet_total"] = DarlingQueryTagRollupReader.FleetTotal(section, facts),
        };
    }

    private static Dictionary<string, object?> WrapDeadlocks(
        bool fleet, IReadOnlyList<DarlingServerResolver.RegisteredServer> scope, Dictionary<int, string> names,
        IReadOnlyList<QueryTagNode> tags, int? tagFilter, int limit, List<QueryTagDeadlockFact> facts, bool scanTruncated)
    {
        var section = QueryTagRollupSection.Deadlocks;
        var status = facts.Count == 0 ? "empty" : "ok";
        var message = facts.Count == 0 ? "No deadlock graphs in the window." : null;
        Dictionary<string, object?> One(IReadOnlyList<QueryTagDeadlockFact> mine) => mine.Count == 0
            ? DarlingQueryTagRollupReader.Empty(section, "empty", "No deadlock graphs in the window.")
            : DarlingQueryTagRollupReader.AssembleDeadlocks(mine, tags, tagFilter, limit);

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
            ["rows_add_up_to_total"] = false,
            ["note"] = "A deadlock is counted once for each tag among its processes, so the deadlock rows do not add up to the total.",
            ["servers"] = servers,
            ["fleet_total"] = DarlingQueryTagRollupReader.FleetDeadlockTotal(facts),
        };
    }
}
