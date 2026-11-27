/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Text.Json.Nodes;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// #9640: Lite's twins of Darling's <c>get_collector_schedules</c> and <c>reset_collector_schedules</c>. Both go through the one
/// <see cref="ScheduleManager"/> the schedule editor and the collection loop use, so a change is saved to
/// collection_schedule.json and read by the running collection loop the way a Save in the editor is.
///
/// <para><b>Lite's schedule is not Darling's.</b> Darling stores override rows per column and a server's value wins column by column.
/// Lite stores one full list for the fleet (the default schedule) and, for a server that has its own schedule, one full
/// list that replaces it whole. So an "override" here is a collector whose enabled flag, frequency, retention or run time differs
/// from the level beneath it (the shipped defaults for the default schedule, the default schedule for a server's own schedule),
/// and Lite has no per-collector database scope to report.</para>
/// </summary>
[McpServerToolType]
public sealed class McpCollectorScheduleTools
{
    private const string Unavailable = "The collector schedules are not available to this MCP server.";

    [McpServerTool(Name = "get_collector_schedules"), Description(
        "Lists the collector schedule overrides: collectors whose enabled flag, frequency_minutes, retention_days or run_at differs from the level beneath, " +
        "for the fleet-wide default schedule (against the shipped defaults) and for each server that has its own schedule (against the default schedule). " +
        "Lite keeps one whole schedule per scope, so a server with its own schedule runs entirely on it and does not follow later changes to the default schedule. " +
        "With collector_name every scope's row for that collector is listed, changed or not, plus the shipped default. " +
        "server_schedules says which servers use the default schedule and which have their own. Darling's per-collector database scope has no Lite field and is not returned. Read only.")]
    public static string GetCollectorSchedules(
        ServerManager serverManager,
        [Description("Optional collector name. Omit to list every collector that differs from the level beneath.")] string? collector_name = null,
        [Description("Optional server name or display name: lists the fleet-wide rows and that server's rows only.")] string? server_name = null,
        ScheduleManager? schedules = null)
    {
        const string Tool = "get_collector_schedules";
        try
        {
            if (schedules is null)
            {
                return McpHelpers.Status("unavailable", Unavailable);
            }

            return Get(schedules, serverManager.GetAllServers(), collector_name, server_name);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError(Tool, ex);
        }
    }

    [McpServerTool(Name = "reset_collector_schedules"), Description(
        "Write: resets the collector schedule of one scope to the shipped defaults, as the schedule editor does. With no server_name it puts the fleet-wide default schedule back to the shipped defaults " +
        "(every collector's enabled flag, frequency, retention and run time); servers with their own schedule are not touched. With server_name it removes that server's own schedule " +
        "(the editor's 'Use default schedule'), so the server follows the default schedule again; the default schedule and other servers are not touched. " +
        "The answer lists what was removed, and it cannot be undone in one call: re-apply any wanted value by hand in Settings > Collector Schedules. " +
        "The running collection loop uses the change at its next cycle. Served only when mcp_allow_writes is true.")]
    public static string ResetCollectorSchedules(
        ServerManager serverManager,
        [Description("Optional server name or display name. Omit to reset the fleet-wide default schedule.")] string? server_name = null,
        ScheduleManager? schedules = null)
    {
        const string Tool = "reset_collector_schedules";
        try
        {
            if (schedules is null)
            {
                return McpHelpers.Status("unavailable", Unavailable);
            }

            return Reset(schedules, serverManager.GetAllServers(), server_name);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError(Tool, ex);
        }
    }

    /// <summary>The server the name picks, over every registered server (the Settings list, enabled or not), or the refusal the
    /// resolver builds for a miss or an ambiguous name.</summary>
    private static (ServerConnection? Server, string? Error) ResolveServer(IReadOnlyList<ServerConnection> servers, string? serverName)
    {
        var (resolved, error) = ServerResolver.ResolveIn(servers, serverName);
        if (error is not null)
        {
            return (null, error);
        }

        return ServerResolver.PickByStorageName(servers, resolved.ServerName);
    }

    private static bool SameValues(CollectorSchedule a, CollectorSchedule b) =>
        a.Enabled == b.Enabled
        && a.FrequencyMinutes == b.FrequencyMinutes
        && a.RetentionDays == b.RetentionDays
        && string.Equals(ScheduleManager.NormalizeRunAt(a.RunAt), ScheduleManager.NormalizeRunAt(b.RunAt), StringComparison.Ordinal);

    private static JsonObject EntryJson(CollectorSchedule s, string scope, string? server) => new()
    {
        ["collector"] = s.Name,
        ["scope"] = scope,
        ["server"] = server,
        ["enabled"] = s.Enabled,
        ["frequency_minutes"] = s.FrequencyMinutes,
        ["retention_days"] = s.RetentionDays,
        ["run_at"] = ScheduleManager.NormalizeRunAt(s.RunAt),
    };

    /// <summary>Rows of <paramref name="own"/> that differ from the same collector in <paramref name="beneath"/>, or every row when
    /// <paramref name="all"/> (a collector was asked for by name).</summary>
    private static IEnumerable<CollectorSchedule> Differing(
        IEnumerable<CollectorSchedule> own, IReadOnlyList<CollectorSchedule> beneath, string? collector, bool all) =>
        own.Where(s => collector is null || string.Equals(s.Name, collector, StringComparison.OrdinalIgnoreCase))
           .Where(s => all
               || beneath.FirstOrDefault(b => string.Equals(b.Name, s.Name, StringComparison.OrdinalIgnoreCase)) is not { } b
               || !SameValues(s, b));

    internal static string Get(ScheduleManager schedules, IReadOnlyList<ServerConnection> servers, string? collectorName, string? serverName)
    {
        var shipped = ScheduleManager.GetDefaultSchedules();

        string? canonical = null;
        if (!string.IsNullOrWhiteSpace(collectorName))
        {
            canonical = shipped.FirstOrDefault(s => string.Equals(s.Name, collectorName.Trim(), StringComparison.OrdinalIgnoreCase))?.Name;
            if (canonical is null)
            {
                /* #9640: Darling's twin answers this with status refused and reason unknown_collector, so a client that handles one handles both. */
                return new JsonObject
                {
                    ["status"] = "refused",
                    ["tool"] = "get_collector_schedules",
                    ["reason"] = "unknown_collector",
                    ["message"] = $"'{collectorName.Trim()}' is not a collector Lite knows. Nothing was changed. Names are the ones get_collection_health reports: " +
                        string.Join(", ", shipped.Select(s => s.Name).OrderBy(n => n, StringComparer.Ordinal)) + ".",
                    ["changed"] = false,
                }.ToJsonString();
            }
        }

        ServerConnection? only = null;
        if (!string.IsNullOrWhiteSpace(serverName))
        {
            var (server, error) = ResolveServer(servers, serverName);
            if (error is not null)
            {
                return error;
            }

            only = server;
        }

        var all = canonical is not null;
        var defaults = schedules.GetDefaultSchedule();
        var entries = new JsonArray();
        foreach (var s in Differing(defaults, shipped, canonical, all))
        {
            entries.Add(EntryJson(s, "fleet-wide", null));
        }

        var serverSchedules = new JsonArray();
        foreach (var server in servers.Where(s => only is null || s.Id == only.Id))
        {
            var own = schedules.HasServerOverride(server.Id);
            var label = server.DisplayName;
            serverSchedules.Add(new JsonObject { ["server"] = label, ["schedule"] = own ? "own" : "default" });
            if (!own)
            {
                continue;
            }

            foreach (var s in Differing(schedules.GetSchedulesForServer(server.Id), defaults, canonical, all))
            {
                entries.Add(EntryJson(s, "server", label));
            }
        }

        var node = new JsonObject
        {
            ["status"] = entries.Count == 0 ? "empty" : "ok",
            ["collector"] = canonical,
            ["server"] = only?.DisplayName,
            ["overrides"] = entries,
            ["server_schedules"] = serverSchedules,
            ["note"] = "A fleet-wide row differs from the shipped defaults. A server row belongs to a server with its own schedule and differs from the default schedule; that server runs entirely on its own schedule. Servers listed as default follow the default schedule.",
        };
        if (canonical is not null)
        {
            var s = shipped.First(x => x.Name == canonical);
            node["code_default"] = new JsonObject
            {
                ["enabled"] = s.Enabled,
                ["frequency_minutes"] = s.FrequencyMinutes,
                ["retention_days"] = s.RetentionDays,
            };
        }

        if (entries.Count == 0)
        {
            node["message"] = "No override is stored here: the shipped defaults apply.";
        }

        return node.ToJsonString(McpHelpers.JsonOptions);
    }

    internal static string Reset(ScheduleManager schedules, IReadOnlyList<ServerConnection> servers, string? serverName)
    {
        const string Tool = "reset_collector_schedules";
        ServerConnection? target = null;
        if (!string.IsNullOrWhiteSpace(serverName))
        {
            var (server, error) = ResolveServer(servers, serverName);
            if (error is not null)
            {
                return error;
            }

            target = server;
        }

        List<CollectorSchedule> removed;
        int scheduleRows;
        bool changed;
        string scopeText;
        if (target is null)
        {
            scopeText = "fleet-wide";
            removed = Differing(schedules.GetDefaultSchedule(), ScheduleManager.GetDefaultSchedules(), null, false).ToList();
            scheduleRows = removed.Count;
            changed = removed.Count > 0;
            if (changed)
            {
                schedules.ResetDefaultSchedule();
            }
        }
        else
        {
            scopeText = target.DisplayName;
            changed = schedules.HasServerOverride(target.Id);
            if (changed)
            {
                var own = schedules.GetSchedulesForServer(target.Id);
                scheduleRows = own.Count;
                removed = Differing(own, schedules.GetDefaultSchedule(), null, false).ToList();
                schedules.RemoveServerOverride(target.Id);
            }
            else
            {
                scheduleRows = 0;
                removed = new List<CollectorSchedule>();
            }
        }

        var runTimes = removed.Count(s => ScheduleManager.NormalizeRunAt(s.RunAt) is not null);
        return new JsonObject
        {
            ["status"] = changed ? "changed" : "unchanged",
            ["tool"] = Tool,
            ["scope"] = scopeText,
            ["what_changed"] = !changed
                ? target is null
                    ? "The default schedule already matches the shipped defaults; nothing was removed."
                    : $"{scopeText} has no schedule of its own; nothing was removed."
                : target is null
                    ? $"The default schedule was put back to the shipped defaults: {scheduleRows} collector row(s) differed."
                    : $"Removed {scopeText}'s own schedule ({scheduleRows} collector rows, {removed.Count} of them different from the default schedule); it follows the default schedule again.",
            ["removed"] = new JsonObject { ["schedule_rows"] = scheduleRows, ["run_times"] = runTimes },
            ["removed_overrides"] = new JsonArray(removed.Select(s => (JsonNode?)EntryJson(s, target is null ? "fleet-wide" : "server", target?.DisplayName)).ToArray()),
            ["notes"] = new JsonArray(
                (JsonNode?)JsonValue.Create(target is null
                    ? "Servers with their own schedule were not touched."
                    : "The default schedule and other servers were not touched."),
                (JsonNode?)JsonValue.Create("This cannot be undone in one call: re-apply any wanted value from removed_overrides in Settings > Collector Schedules.")),
            ["takes_effect"] = "The collection loop reads the schedule at its next cycle; no restart is needed.",
        }.ToJsonString(McpHelpers.JsonOptions);
    }
}
