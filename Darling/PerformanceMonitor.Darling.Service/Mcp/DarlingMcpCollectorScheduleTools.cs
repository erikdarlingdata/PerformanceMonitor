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
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The collector schedule tools (#6227): <c>disable_collector</c>, <c>enable_collector</c>, <c>set_collector_frequency</c>,
/// <c>set_collector_run_at</c> and <c>set_collector_retention</c> change one collector's row in
/// <c>config.config_collector_schedules</c> (or <c>config_collector_run_times</c>), fleet-wide or for one server, and
/// <c>get_collector_schedules</c> reads the overrides back. The writes are served under <c>mcp.allowWrites</c> and are never offered
/// to an investigation agent (<see cref="DarlingMcpWritePolicy.NeverAgentTools"/>).
///
/// <para><b>One write path.</b> Each tool builds the <see cref="ClaimedCommand"/> a queued command row would carry and asks
/// <see cref="DarlingCommandExecutor.ResolvePlan"/> for the plan, which is where the collector-name check, the bounds
/// (<c>CollectorScheduleBounds</c>) and the SQL live, and runs it through
/// <see cref="DarlingCommandExecutor.ExecuteStoreWriteAsync(NpgsqlDataSource, CommandPlan, CancellationToken)"/>: the same plan the
/// <c>--enable-collector</c> / <c>--set-collector-run-at</c> verbs execute. The server is resolved by the write rule the verbs use
/// (<see cref="DarlingCliCommands.ResolveCollectorScopeAsync"/>), and a run time is judged against the collector's interval by
/// <see cref="DarlingCliCommands.RunAtIntervalRefusal"/>. This class owns no SQL and no validation of its own.</para>
///
/// <para><b>Every answer says what changed, what it was, and the call that puts it back.</b> The service re-reads schedules on its
/// next config poll, so there is no restart.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpCollectorScheduleTools
{
    private const string TakesEffect = "The running service re-reads schedules on its next config poll; no restart is needed.";

    private enum Change
    {
        Enable,
        Disable,
        Frequency,
        RunAt,
        Retention,
    }

    /// <summary>One requested change. <see cref="Number"/> is the frequency or retention; <see cref="Clear"/> asks for the column to
    /// go back to the next level; <see cref="RunAt"/> is the run time as typed.</summary>
    private sealed record Edit(Change Kind, string Tool, string? Collector, string? Server, int? Number, bool Clear, string? RunAt, bool Inherit = false);

    [McpServerTool(Name = "disable_collector"), Description(
        "Stops one collector: sets its enabled flag to false, for every monitored server (no server_name) or for ONE server. Nothing else on its row changes and data already collected stays. The service picks it up on its next config poll; no restart. The answer gives the previous state and the exact call that undoes it. Needs mcp.allowWrites; never offered to an investigation agent. inherit=true puts the enabled flag back to inherited (a server then follows the fleet-wide row again). Read the current state with get_collector_schedules; collector names are in get_collection_health.")]
    public static Task<string> DisableCollector(
        NpgsqlDataSource postgres,
        [Description("Collector name, as get_collection_health reports it.")] string collector_name,
        [Description("Optional server name or display name. Omit to change the fleet-wide row.")] string? server_name = null,
        [Description("True to put the row's enabled flag back to 'inherited' (the fleet-wide value for a server, else the shipped default) instead of setting it, so a later fleet-wide change reaches it again.")] bool inherit = false,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        ApplyAsync(postgres, new Edit(Change.Disable, "disable_collector", collector_name, server_name, null, false, null, inherit), logger, cancellationToken);

    [McpServerTool(Name = "enable_collector"), Description(
        "Starts one collector: sets its enabled flag to true, for every monitored server (no server_name) or for ONE server. Nothing else on its row changes. A collector that ships off (opt-in) is switched on by this. The service picks it up on its next config poll; no restart. The answer gives the previous state and the exact call that undoes it. inherit=true puts the enabled flag back to inherited (a server then follows the fleet-wide row again). Needs mcp.allowWrites; never offered to an investigation agent.")]
    public static Task<string> EnableCollector(
        NpgsqlDataSource postgres,
        [Description("Collector name, as get_collection_health reports it.")] string collector_name,
        [Description("Optional server name or display name. Omit to change the fleet-wide row.")] string? server_name = null,
        [Description("True to put the row's enabled flag back to 'inherited' (the fleet-wide value for a server, else the shipped default) instead of setting it, so a later fleet-wide change reaches it again.")] bool inherit = false,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        ApplyAsync(postgres, new Edit(Change.Enable, "enable_collector", collector_name, server_name, null, false, null, inherit), logger, cancellationToken);

    [McpServerTool(Name = "set_collector_frequency"), Description(
        "Sets, or clears, how often one collector runs, in minutes (0 = once when the server loads), fleet-wide (no server_name) or for ONE server. clear=true removes the override so the next level applies (the fleet row for a server, else the code default). Same bounds as the Viewer's schedule editor: not negative, and a delta-family collector accepts at most the delta cap. Only frequency changes; the enabled flag and retention on the row are kept, and a new row never switches a collector on or off. The answer gives the previous value and the exact call that undoes it. Needs mcp.allowWrites; never offered to an investigation agent.")]
    public static Task<string> SetCollectorFrequency(
        NpgsqlDataSource postgres,
        [Description("Collector name, as get_collection_health reports it.")] string collector_name,
        [Description("Minutes between runs, 0 or more. Omit when clear is true.")] int? frequency_minutes = null,
        [Description("True to clear the override instead of setting a value.")] bool clear = false,
        [Description("Optional server name or display name. Omit to change the fleet-wide row.")] string? server_name = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        ApplyAsync(postgres, new Edit(Change.Frequency, "set_collector_frequency", collector_name, server_name, frequency_minutes, clear, null), logger, cancellationToken);

    [McpServerTool(Name = "set_collector_run_at"), Description(
        "Sets, stops or clears the time of day a collector that runs once a day or less often starts, fleet-wide (no server_name) or for ONE server. run_at is HH:MM (24-hour, the monitored server's own clock), 'none' (on a server: do not use the fleet's time) or 'default' (clear this row's time so the next level applies). A time on a collector that runs more often than daily is refused. Only the run time changes. The answer gives the previous value and the exact call that undoes it. Needs mcp.allowWrites; never offered to an investigation agent.")]
    public static Task<string> SetCollectorRunAt(
        NpgsqlDataSource postgres,
        [Description("Collector name, as get_collection_health reports it.")] string collector_name,
        [Description("HH:MM, 'none' or 'default'.")] string run_at,
        [Description("Optional server name or display name. Omit to change the fleet-wide row.")] string? server_name = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        ApplyAsync(postgres, new Edit(Change.RunAt, "set_collector_run_at", collector_name, server_name, null, false, run_at), logger, cancellationToken);

    [McpServerTool(Name = "set_collector_retention"), Description(
        "Sets, or clears, how many days one collector's data is kept, fleet-wide (no server_name) or for ONE server. clear=true removes the override so the next level applies (the fleet row for a server, else the code default). Same bound as the Viewer's schedule editor: at least 1 day. Only retention changes; the enabled flag and frequency on the row are kept, and a new row never switches a collector on or off. A shorter retention lets the next purge delete older rows. The answer gives the previous value and the exact call that undoes it. Needs mcp.allowWrites; never offered to an investigation agent.")]
    public static Task<string> SetCollectorRetention(
        NpgsqlDataSource postgres,
        [Description("Collector name, as get_collection_health reports it.")] string collector_name,
        [Description("Days to keep, 1 or more. Omit when clear is true.")] int? retention_days = null,
        [Description("True to clear the override instead of setting a value.")] bool clear = false,
        [Description("Optional server name or display name. Omit to change the fleet-wide row.")] string? server_name = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        ApplyAsync(postgres, new Edit(Change.Retention, "set_collector_retention", collector_name, server_name, retention_days, clear, null), logger, cancellationToken);

    [McpServerTool(Name = "get_collector_schedules"), Description(
        "Lists the schedule overrides stored for one collector, or for every collector: enabled, frequency_minutes, retention_days, run_at and the database scope, fleet-wide or per server. A server's own value wins over the fleet-wide one, column by column; a missing value means the next level applies, and the code default applies where there is no row; an enabled flag stored as NULL shows as inherited. With collector_name the answer also gives that collector's code default. Use it before and after disable_collector / enable_collector / set_collector_frequency / set_collector_run_at / set_collector_retention. Read only.")]
    public static async Task<string> GetCollectorSchedules(
        NpgsqlDataSource postgres,
        [Description("Optional collector name. Omit to list every collector that has an override.")] string? collector_name = null,
        [Description("Optional server name or display name: lists the fleet-wide rows and that server's rows only.")] string? server_name = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            string? canonical = null;
            if (!string.IsNullOrWhiteSpace(collector_name))
            {
                canonical = DarlingCliCommands.CanonicalCollectorName(collector_name);
                if (canonical is null)
                {
                    return Refuse("get_collector_schedules", "unknown_collector", UnknownCollectorMessage(collector_name));
                }
            }

            int? serverId = null;
            string? serverLabel = null;
            if (!string.IsNullOrWhiteSpace(server_name))
            {
                var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
                if (error is not null)
                {
                    return error;
                }

                serverId = resolved.ServerId;
                serverLabel = resolved.DisplayName;
            }

            var rows = await DarlingCliCommands.ReadCollectorScheduleRowsAsync(postgres, canonical, cancellationToken);
            var runTimes = await DarlingCliCommands.ReadCollectorRunTimeRowsAsync(postgres, canonical, cancellationToken);
            var entries = Merge(rows, runTimes)
                .Where(e => serverId is null || e.ServerId is null || e.ServerId == serverId)
                .ToList();

            var node = new JsonObject
            {
                ["status"] = entries.Count == 0 ? "empty" : "ok",
                ["collector"] = canonical,
                ["server"] = serverLabel,
                ["overrides"] = new JsonArray(entries.Select(EntryJson).ToArray<JsonNode?>()),
                ["note"] = "A server's own value wins over the fleet-wide one, column by column. null = no value at that level, so the next level applies; with no row the code default applies.",
            };
            if (canonical is not null && CollectorScheduleDefaults.All.TryGetValue(canonical, out var def))
            {
                node["code_default"] = new JsonObject
                {
                    ["enabled"] = def.DefaultEnabled,
                    ["frequency_minutes"] = def.FrequencyMinutes,
                    ["retention_days"] = def.RetentionDays,
                };
            }

            if (entries.Count == 0)
            {
                node["message"] = "No override is stored here: the code defaults apply.";
            }

            return node.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_collector_schedules", ex);
        }
    }

    /// <summary>One scope's view of a collector: its schedule row and its run-time row, either of which may be absent.</summary>
    private sealed record Entry(
        int? ServerId, string Collector, string? ServerLabel, bool? Enabled, int? FrequencyMinutes, int? RetentionDays,
        IReadOnlyList<string>? Databases, int? RunAtMinute, bool HasScheduleRow = true);

    private static List<Entry> Merge(
        IReadOnlyList<DarlingCliCommands.CollectorScheduleReadbackRow> rows, IReadOnlyList<DarlingCliCommands.CollectorRunTimeReadbackRow> runTimes)
    {
        var entries = new List<Entry>();
        foreach (var row in rows)
        {
            var runAt = runTimes.FirstOrDefault(r => r.ServerId == row.ServerId
                && string.Equals(r.CollectorName, row.CollectorName, StringComparison.OrdinalIgnoreCase));
            entries.Add(new Entry(row.ServerId, row.CollectorName, row.ServerLabel, row.Enabled, row.FrequencyMinutes, row.RetentionDays, row.Databases, runAt?.RunAtMinute));
        }

        foreach (var time in runTimes)
        {
            if (!rows.Any(r => r.ServerId == time.ServerId && string.Equals(r.CollectorName, time.CollectorName, StringComparison.OrdinalIgnoreCase)))
            {
                entries.Add(new Entry(time.ServerId, time.CollectorName, time.ServerLabel, null, null, null, null, time.RunAtMinute, false));
            }
        }

        return entries
            .OrderBy(e => e.Collector, StringComparer.Ordinal)
            .ThenBy(e => e.ServerId is null ? 0 : 1)
            .ThenBy(e => e.ServerLabel, StringComparer.Ordinal)
            .ToList();
    }

    private static JsonNode EntryJson(Entry e) => new JsonObject
    {
        ["collector"] = e.Collector,
        ["scope"] = e.ServerId is null ? "fleet-wide" : "server",
        ["server"] = e.ServerId is null ? null : e.ServerLabel ?? $"server_id {e.ServerId}",
        /* V206 (#6227): a schedule row whose enabled flag is NULL inherits (the fleet-wide row, then the shipped default). */
        ["enabled"] = e.Enabled is bool enabled ? enabled : e.HasScheduleRow ? "inherited" : null,
        ["frequency_minutes"] = e.FrequencyMinutes,
        ["retention_days"] = e.RetentionDays,
        ["run_at"] = e.RunAtMinute is int m ? RunAtText(m) : null,
        ["databases"] = e.Databases is null ? null : new JsonArray(e.Databases.Select(d => (JsonNode?)JsonValue.Create(d)).ToArray()),
    };

    /// <summary>A stored run-time minute as the tool says it: <c>HH:MM</c>, or <c>none</c> for -1 (a server row that stops the
    /// fleet's time).</summary>
    private static string RunAtText(int minute) => minute < 0 ? "none" : CollectorRunTime.Format(minute);

    private static string UnknownCollectorMessage(string typed) =>
        $"'{typed}' is not a collector this service knows. Nothing was changed. Names are the ones get_collection_health reports: " +
        string.Join(", ", CollectorScheduleDefaults.All.Keys.OrderBy(k => k, StringComparer.Ordinal)) + ".";

    private static string Refuse(string tool, string reason, string message) =>
        new JsonObject
        {
            ["status"] = "refused",
            ["tool"] = tool,
            ["reason"] = reason,
            ["message"] = message,
            ["changed"] = false,
        }.ToJsonString();

    /// <summary>The command a queued command row would carry for this edit, so the executor's plan is the one asked.</summary>
    private static ClaimedCommand BuildCommand(Edit edit, string collector, int? serverId)
    {
        var args = new Dictionary<string, object?> { ["collector_name"] = collector };
        string type;
        switch (edit.Kind)
        {
            case Change.Enable:
                type = "enable_collector";
                if (edit.Inherit)
                {
                    args["inherit"] = true;
                }

                break;
            case Change.Disable:
                type = "disable_collector";
                if (edit.Inherit)
                {
                    args["inherit"] = true;
                }

                break;
            case Change.Frequency:
                type = "set_collector_frequency";
                args["frequency_minutes"] = edit.Clear ? "default" : edit.Number;
                break;
            case Change.Retention:
                type = "set_collector_retention";
                args["retention_days"] = edit.Clear ? "default" : edit.Number;
                break;
            default:
                type = "set_collector_run_at";
                args["run_at"] = edit.RunAt;
                break;
        }

        return new ClaimedCommand(0, type, serverId, JsonSerializer.Serialize(args), "mcp");
    }

    private static async Task<string> ApplyAsync(NpgsqlDataSource postgres, Edit edit, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            var canonical = DarlingCliCommands.CanonicalCollectorName(edit.Collector);
            if (canonical is null)
            {
                return Refuse(edit.Tool, "unknown_collector", UnknownCollectorMessage(edit.Collector ?? string.Empty));
            }

            if (edit.Inherit && edit.Kind is not (Change.Enable or Change.Disable))
            {
                return Refuse(edit.Tool, "invalid", "inherit only applies to enable_collector and disable_collector. Nothing was changed.");
            }

            if (edit.Kind is Change.Frequency or Change.Retention && edit.Clear == edit.Number.HasValue)
            {
                var column = edit.Kind == Change.Frequency ? "frequency_minutes" : "retention_days";
                return Refuse(edit.Tool, "invalid", $"{edit.Tool} needs either {column} (a whole number) or clear=true, not both and not neither. Nothing was changed.");
            }

            /* The executor's own plan first, for a fleet scope: it refuses a bad value in its own words before the store is touched. */
            var preflight = DarlingCommandExecutor.ResolvePlan(BuildCommand(edit, canonical, null));
            if (preflight.Kind != CommandKind.StoreWrite)
            {
                return Refuse(edit.Tool, "invalid", (preflight.FailReason ?? "The request was refused.") + " Nothing was changed.");
            }

            DarlingCliCommands.CollectorScope scope;
            using (var scopeError = new StringWriter())
            {
                scope = await DarlingCliCommands.ResolveCollectorScopeAsync(postgres, string.IsNullOrWhiteSpace(edit.Server) ? null : edit.Server, scopeError);
                if (scope.FailureExitCode is not null)
                {
                    return Refuse(edit.Tool, "server", scopeError.ToString().Trim());
                }
            }

            var rowsBefore = await DarlingCliCommands.ReadCollectorScheduleRowsAsync(postgres, canonical, cancellationToken);
            var timesBefore = await DarlingCliCommands.ReadCollectorRunTimeRowsAsync(postgres, canonical, cancellationToken);

            if (edit.Kind == Change.RunAt
                && CollectorRunTime.TryParse(edit.RunAt, out _)
                && DarlingCliCommands.RunAtIntervalRefusal(canonical, scope.ServerId, rowsBefore) is string intervalRefusal)
            {
                return Refuse(edit.Tool, "invalid", intervalRefusal + " Nothing was changed.");
            }

            try
            {
                await DarlingCommandExecutor.ExecuteStoreWriteAsync(
                    postgres, DarlingCommandExecutor.ResolvePlan(BuildCommand(edit, canonical, scope.ServerId)), cancellationToken);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable && edit.Kind == Change.RunAt)
            {
                return Refuse(edit.Tool, "store_not_ready", DarlingCliCommands.RunTimeTableMissingMessage("set_collector_run_at") + " Nothing was changed.");
            }

            var rowsAfter = await DarlingCliCommands.ReadCollectorScheduleRowsAsync(postgres, canonical, cancellationToken);
            var timesAfter = await DarlingCliCommands.ReadCollectorRunTimeRowsAsync(postgres, canonical, cancellationToken);
            var answer = Describe(edit, canonical, scope, rowsBefore, timesBefore, rowsAfter, timesAfter);
            LogWrite(logger, edit, canonical, scope, rowsBefore, timesBefore, rowsAfter, timesAfter);
            return answer;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(edit.Tool, ex);
        }
    }

    /// <summary>One log line per schedule write: the tool, the collector, the scope, the value before and after, and the caller the
    /// request filter worked out (<see cref="ReadCaller.AmbientMcpCaller"/>: the client name and version the connection sent). Written
    /// at Information, after the write, from the rows read before and after it, so the line says what the store held and not what was
    /// asked for.</summary>
    private static void LogWrite(
        ILogger? logger, Edit edit, string collector, DarlingCliCommands.CollectorScope scope,
        IReadOnlyList<DarlingCliCommands.CollectorScheduleReadbackRow> rowsBefore, IReadOnlyList<DarlingCliCommands.CollectorRunTimeReadbackRow> timesBefore,
        IReadOnlyList<DarlingCliCommands.CollectorScheduleReadbackRow> rowsAfter, IReadOnlyList<DarlingCliCommands.CollectorRunTimeReadbackRow> timesAfter)
    {
        if (logger is null)
        {
            return;
        }

        var serverId = scope.ServerId;
        var rowBefore = rowsBefore.FirstOrDefault(r => r.ServerId == serverId);
        var rowAfter = rowsAfter.FirstOrDefault(r => r.ServerId == serverId);
        string before;
        string after;
        switch (edit.Kind)
        {
            case Change.Enable:
            case Change.Disable:
                before = EnabledText(rowBefore);
                after = EnabledText(rowAfter);
                break;
            case Change.Frequency:
                before = NumberText(rowBefore, rowBefore?.FrequencyMinutes);
                after = NumberText(rowAfter, rowAfter?.FrequencyMinutes);
                break;
            case Change.Retention:
                before = NumberText(rowBefore, rowBefore?.RetentionDays);
                after = NumberText(rowAfter, rowAfter?.RetentionDays);
                break;
            default:
                before = timesBefore.FirstOrDefault(t => t.ServerId == serverId)?.RunAtMinute is int bm ? RunAtText(bm) : "none stored";
                after = timesAfter.FirstOrDefault(t => t.ServerId == serverId)?.RunAtMinute is int am ? RunAtText(am) : "none stored";
                break;
        }

        logger.LogInformation(
            "MCP schedule write {Tool}{Inherit}: collector {Collector}, scope {Scope}, before {Before}, after {After}, caller {Caller}",
            edit.Tool, edit.Inherit ? " (inherit)" : string.Empty, collector, scope.Label, before, after, ReadCaller.AmbientMcpCaller ?? ReadCaller.Unknown);
    }

    /// <summary>A server as a note names it: the storage name the server resolver accepts (the label, "Display (Storage)", is not
    /// one), else the id for a row whose server is not in the registry.</summary>
    private static string NoteNameOf(DarlingCliCommands.CollectorScheduleReadbackRow row) =>
        row.ServerName ?? $"server_id {row.ServerId}";

    private static string EnabledText(DarlingCliCommands.CollectorScheduleReadbackRow? row) =>
        row is null ? "no row" : row.Enabled is bool b ? (b ? "enabled" : "disabled") : "inherited";

    private static string NumberText(DarlingCliCommands.CollectorScheduleReadbackRow? row, int? value) =>
        row is null ? "no row" : value is int v ? v.ToString(System.Globalization.CultureInfo.InvariantCulture) : "not set";

    /// <summary>The answer: what changed, what it was, the call that undoes it, and anything the caller should know. Pure over the rows
    /// read before and after, so a test can drive it.</summary>
    private static string Describe(
        Edit edit, string collector, DarlingCliCommands.CollectorScope scope,
        IReadOnlyList<DarlingCliCommands.CollectorScheduleReadbackRow> rowsBefore, IReadOnlyList<DarlingCliCommands.CollectorRunTimeReadbackRow> timesBefore,
        IReadOnlyList<DarlingCliCommands.CollectorScheduleReadbackRow> rowsAfter, IReadOnlyList<DarlingCliCommands.CollectorRunTimeReadbackRow> timesAfter)
    {
        var serverId = scope.ServerId;
        var def = CollectorScheduleDefaults.All[collector];
        var rowBefore = rowsBefore.FirstOrDefault(r => r.ServerId == serverId);
        var rowAfter = rowsAfter.FirstOrDefault(r => r.ServerId == serverId);
        var fleetBefore = rowsBefore.FirstOrDefault(r => r.ServerId is null);
        var scopeText = serverId is null ? "fleet-wide" : scope.Label;

        string tool;
        var undoArgs = new JsonObject { ["collector_name"] = collector };
        if (serverId is not null)
        {
            /* The storage name, not the label: the label is "Display (ServerName)", which the server resolver does not accept. */
            undoArgs["server_name"] = scope.ServerName ?? scope.Label;
        }

        object? previous;
        object? current;
        string what;
        var notes = new List<string>();
        bool changed;

        switch (edit.Kind)
        {
            case Change.Enable:
            case Change.Disable:
            {
                var wanted = edit.Kind == Change.Enable;
                /* The state the scope resolved to before: its own row, else (for a server) the fleet row, else the code default. A NULL
                   enabled flag (V206) inherits, so it falls through like a missing row. */
                var effectiveBefore = rowBefore?.Enabled ?? (serverId is null ? null : fleetBefore?.Enabled) ?? def.DefaultEnabled;
                previous = new { enabled = rowBefore?.Enabled, inherited = rowBefore is not null && rowBefore.Enabled is null, effective_enabled = effectiveBefore };
                /* The undo puts back what the row held: an explicit true or false, or "inherited" (no explicit value). */
                tool = rowBefore?.Enabled is bool was ? (was ? "enable_collector" : "disable_collector") : (effectiveBefore ? "enable_collector" : "disable_collector");
                if (rowBefore?.Enabled is null)
                {
                    undoArgs["inherit"] = true;
                }

                if (edit.Inherit)
                {
                    var effectiveAfter = (serverId is null ? null : rowsAfter.FirstOrDefault(r => r.ServerId is null)?.Enabled) ?? def.DefaultEnabled;
                    current = new { enabled = rowAfter?.Enabled, inherited = rowAfter is not null && rowAfter.Enabled is null, effective_enabled = effectiveAfter };
                    what = $"{collector} enabled state now inherits {scopeText}; it resolves to {(effectiveAfter ? "enabled" : "disabled")} ({(serverId is null ? "the code default" : "the fleet-wide row's value, else the code default")}).";
                    changed = rowBefore?.Enabled is not null;
                    if (rowBefore is null)
                    {
                        notes.Add($"There was no {(serverId is null ? "fleet-wide" : "server")} row, so nothing was written.");
                    }

                    break;
                }

                current = new { enabled = rowAfter?.Enabled };
                what = $"{collector} is now {(wanted ? "enabled" : "disabled")} {scopeText}.";
                changed = rowBefore?.Enabled != wanted;
                if (rowBefore is null)
                {
                    notes.Add($"There was no {(serverId is null ? "fleet-wide" : "server")} row; one was created. Before it, the collector resolved to {(effectiveBefore ? "enabled" : "disabled")} here ({(serverId is null || fleetBefore is null ? "the code default" : "the fleet-wide row")}), and the row now stores an explicit value that wins over any later fleet-wide change. inherit=true puts it back.");
                }

                if (serverId is null)
                {
                    var own = rowsAfter.Where(r => r.ServerId is not null && r.Enabled is bool theirs && theirs != wanted)
                        .Select(NoteNameOf).ToList();
                    if (own.Count > 0)
                    {
                        notes.Add($"These servers have their own enabled value, which wins over the fleet-wide one: {string.Join(", ", own)}. enable_collector / disable_collector with inherit=true and a server_name puts a server back on the fleet-wide value.");
                    }
                }

                break;
            }

            case Change.Frequency:
            case Change.Retention:
            {
                var freq = edit.Kind == Change.Frequency;
                var name = freq ? "frequency_minutes" : "retention_days";
                var before = freq ? rowBefore?.FrequencyMinutes : rowBefore?.RetentionDays;
                var after = freq ? rowAfter?.FrequencyMinutes : rowAfter?.RetentionDays;
                previous = freq ? new { frequency_minutes = before } : new { retention_days = before };
                current = freq ? new { frequency_minutes = after } : new { retention_days = after };
                tool = freq ? "set_collector_frequency" : "set_collector_retention";
                if (before is int b && (freq ? CollectorScheduleBounds.FrequencyError(collector, b) : CollectorScheduleBounds.RetentionError(collector, b)) is null)
                {
                    undoArgs[name] = b;
                }
                else
                {
                    undoArgs["clear"] = true;
                    if (before is int tooBig)
                    {
                        /* A value stored before today's limits (or written straight into the store) is one this tool now refuses, so the
                           undo it names must not be that call. */
                        notes.Add($"The previous {name} ({tooBig}) is above what this tool accepts today, so the old value cannot be put back with it; the undo clears the override, and the {(serverId is null ? "code default" : "fleet-wide value, else the code default")} applies.");
                    }
                }

                changed = before != after;
                what = after is int a
                    ? $"{collector} {name} is now {a} {scopeText}."
                    : $"{collector} {name} override cleared {scopeText}; the {(serverId is null ? "code default" : "fleet-wide value, else the code default")} applies.";
                if (after is null && rowBefore is null)
                {
                    notes.Add("There was no override at this scope, so nothing was written.");
                }

                var codeDefault = freq ? def.FrequencyMinutes : def.RetentionDays;
                notes.Add($"Code default: {codeDefault}.");
                if (serverId is null)
                {
                    var own = rowsAfter.Where(r => r.ServerId is not null && (freq ? r.FrequencyMinutes : r.RetentionDays) is not null)
                        .Select(NoteNameOf).ToList();
                    if (own.Count > 0)
                    {
                        notes.Add($"These servers have their own {name}, which wins over the fleet-wide one: {string.Join(", ", own)}.");
                    }
                }

                if (freq)
                {
                    var runAt = timesAfter.FirstOrDefault(t => t.ServerId == serverId)?.RunAtMinute
                        ?? (serverId is null ? null : timesAfter.FirstOrDefault(t => t.ServerId is null)?.RunAtMinute);
                    if (runAt is >= 0 && !CollectorRunTime.AllowsRunAt(DarlingCliCommands.ResolveRunAtInterval(collector, serverId, rowsAfter)))
                    {
                        notes.Add($"WARNING: this collector has a fixed run time ({RunAtText(runAt.Value)}), and a run time only applies to a collector that runs once a day or less often, so the service now ignores it. Clear it with set_collector_run_at run_at=default, or put the frequency back.");
                    }
                }

                break;
            }

            default:
            {
                var before = timesBefore.FirstOrDefault(t => t.ServerId == serverId)?.RunAtMinute;
                var after = timesAfter.FirstOrDefault(t => t.ServerId == serverId)?.RunAtMinute;
                previous = new { run_at = before is int bm ? RunAtText(bm) : null };
                current = new { run_at = after is int am ? RunAtText(am) : null };
                tool = "set_collector_run_at";
                undoArgs["run_at"] = before is int pm ? RunAtText(pm) : "default";
                changed = before != after;
                what = after is int set
                    ? $"{collector} run time is now {RunAtText(set)}{(set >= 0 ? " (the monitored server's own clock)" : string.Empty)} {scopeText}."
                    : $"{collector} run time row cleared {scopeText}; the {(serverId is null ? "collector runs on its interval again" : "fleet-wide time, if any, applies")}.";
                break;
            }
        }

        return new JsonObject
        {
            ["status"] = changed ? "changed" : "unchanged",
            ["tool"] = edit.Tool,
            ["collector"] = collector,
            ["scope"] = scopeText,
            ["what_changed"] = what,
            ["previous"] = JsonSerializer.SerializeToNode(previous),
            ["current"] = JsonSerializer.SerializeToNode(current),
            ["undo"] = new JsonObject { ["tool"] = tool, ["arguments"] = undoArgs },
            ["notes"] = new JsonArray(notes.Select(n => (JsonNode?)JsonValue.Create(n)).ToArray()),
            ["takes_effect"] = TakesEffect,
        }.ToJsonString();
    }
}
