/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The Default Trace MCP tool — <c>get_default_trace_events</c>, the same name the Dashboard's
/// <c>McpSystemEventTools</c> exposes — served over Darling's Postgres store (a STORED read, no live
/// monitored-server hit). Where the Dashboard reads its server-side <c>collect.default_trace_events</c>
/// table, this reads Darling's collected copy (<see cref="DarlingDefaultTraceReader"/>) and returns the
/// SIGNIFICANT set, gating each row through the shared <see cref="DefaultTraceEventSignificance"/> — the
/// exact same significant-set gate the viewer's System Events surface uses, so the tool and the viewer
/// agree. Each row is tagged with its <see cref="DefaultTraceEventCategory"/>.
///
/// <para>The collector already curates the event set server-side (auto-grow/shrink only when a stall &gt; 1 s,
/// tempdb / <c>_WA_</c> auto-stats DDL excluded), so the one filter this layer adds is the ErrorLog severity
/// floor. <b>Config-change events are intentionally NOT here</b> — Darling tracks that slice via the config
/// snapshot diffs, so use <c>get_server_config_changes</c> / <c>get_database_config_changes</c> /
/// <c>get_trace_flag_changes</c> for configuration changes.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpDefaultTraceTools
{
    [McpServerTool(Name = "get_default_trace_events"), Description("Gets significant server events captured by the built-in Default Trace (stored, read-only): data/log file auto-grow/shrink STALLS (over 1 second), severe ErrorLog writes (severity >= 16), schema DDL (object create/alter/delete), security audits (audit-change / DBCC / alter-trace), and Server Memory Change. Each event is tagged with a category. NOTE: configuration-change events are intentionally excluded here to avoid double-counting — use get_server_config_changes / get_database_config_changes / get_trace_flag_changes for those. Not available on Azure SQL Database (no default trace there).")]
    public static async Task<string> GetDefaultTraceEvents(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of events to return. Default 100.")] int limit = 100,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var all = await DarlingDefaultTraceReader.ReadEventsAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);

            /* The significant-set gate (shared with the viewer's System Events surface): every curated
               category is significant as collected, except ErrorLog which must clear the severity floor. */
            var significant = all
                .Where(r => DefaultTraceEventSignificance.IsSignificant(r.EventName, r.Severity))
                .ToList();

            if (significant.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "default_trace_events")
                    ?? McpHelpers.Status("empty", "No significant default trace events found in the requested time range.");

            var events = significant.Take(limit).Select(r =>
            {
                var category = DefaultTraceEventSignificance.Classify(r.EventName);
                return new
                {
                    event_time = r.EventTime?.ToString("o"),
                    category = category.ToString(),
                    event_name = r.EventName,
                    event_class = r.EventClass,
                    database_name = r.DatabaseName,
                    object_name = r.ObjectName,
                    login_name = r.LoginName,
                    host_name = r.HostName,
                    application_name = r.ApplicationName,
                    spid = r.Spid,
                    filename = r.Filename,
                    /* Duration is meaningful for the auto-grow/shrink stalls (how long the growth blocked). */
                    duration_ms = r.DurationUs.HasValue ? r.DurationUs.Value / 1000.0 : (double?)null,
                    /* IntegerData is the 8-KB page count on an auto-grow/shrink event -> the grown/shrunk size. */
                    growth_mb = category == DefaultTraceEventCategory.AutoGrowShrink && r.IntegerData.HasValue
                        ? r.IntegerData.Value * 8.0 / 1024.0
                        : (double?)null,
                    error_number = r.ErrorNumber,
                    severity = r.Severity,
                    text_data = McpHelpers.Truncate(r.TextData, 2000)
                };
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                total_events = significant.Count,
                shown = events.Count,
                events
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_default_trace_events", ex);
        }
    }
}
