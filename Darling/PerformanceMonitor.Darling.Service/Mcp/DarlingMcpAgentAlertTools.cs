/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #6874: <c>get_agent_alerts</c>, the SQL Server Agent alerts defined on an instance and the gaps in them. A STORED read of the
/// <c>agent_alerts</c> collector through <see cref="DarlingAgentAlertReader"/>; the instance status is the pure
/// <see cref="AgentAlertStatus.Evaluate"/>, so the Viewer can say the same words.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpAgentAlertTools
{
    private static string? Format(DateTime? t) =>
        t is { } v ? DateTime.SpecifyKind(v, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture) : null;

    /// <summary>msdb stores alert times in the monitored server's own clock, so they carry no zone and no Z.</summary>
    private static string? FormatServerLocal(DateTime? t) => t?.ToString("s", CultureInfo.InvariantCulture);

    [McpServerTool(Name = "get_agent_alerts"), Description(
        "The SQL Server Agent alerts defined on an instance, with an instance status that names what they lack. <<GUIDE>> " +
        "instance_status is ok or gaps. gaps[] names each gap by code: missing_severity_alerts (severities 19 to 25 with no alert), " +
        "missing_error_alerts (errors 823, 824, 825 with no alert), alert_without_operator, alert_disabled, fired_unnotified " +
        "(the alert's count rose in the window and it tells no operator), no_alerts_defined (the instance has no alert at all: a gap, not empty data). " +
        "alerts[] is the newest capture in the window, with fired_in_window from the change in occurrence_count (null with only one capture). " +
        "Reads the agent_alerts collector (every 15 minutes), never a monitored server. Status words: unavailable (no capture in the window), " +
        "precondition (the collector was refused or is switched off for this server), not_collected (Azure SQL Database and Managed Instance have no Agent alerts, and the collector does not run on Amazon RDS).")]
    public static async Task<string> GetAgentAlerts(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of captures ending at as_of (default 24). Fired is judged between the window's first and last capture.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        var start = windowEnd.AddHours(-hours_back);

        try
        {
            var read = await DarlingAgentAlertReader.ReadAsync(
                postgres, resolved.ServerId, start, windowEnd, McpCommandDeadlines.ReadSeconds, cancellationToken);
            ReadScope.NoteSource(ReadScope.SourceRaw);

            if (!read.HasCapture)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "agent_alerts", cancellationToken)
                    ?? await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "agent_alerts", cancellationToken)
                    /* #6874: the gate is !IsAzureSqlDb && !IsAwsRds && !IsAzureManagedInstance. The engine half is answered above, so AWS RDS
                       is the one candidate left. Since #6894 the registry records the RDS fact on every connect and the capability read above answers
                       not_collected for it, so this route is reached only by a server no connect has stamped yet (or a read that failed). The
                       gated-off route is the one get_running_jobs uses for the same case. */
                    ?? await DarlingRuntimePrecondition.GatedOffStatusAsync(
                        postgres, resolved.ServerId, resolved.DisplayName, "agent_alerts",
                        CollectorRuntimePrecondition.AgentAlertsPossibleCauses, cancellationToken)
                    ?? McpHelpers.Status("unavailable",
                        $"No SQL Server Agent alert capture in the {hours_back}-hour window ending {Format(windowEnd)}. The agent_alerts collector has not run in it.");
            }

            var status = AgentAlertStatus.Evaluate(read.Alerts);
            return JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                as_of = Format(windowEnd),
                hours_back,
                captured_at = Format(read.NewestCapture),
                oldest_capture_in_window = Format(read.OldestCapture),
                captures_in_window = read.Captures,
                alert_count = read.Alerts.Count,
                instance_status = status.Word,
                gaps = status.Gaps.Select(g => new { code = g.Code, message = g.Message, alerts = g.Alerts, missing = g.Missing }),
                alerts = read.Alerts.Select(a => new
                {
                    alert_id = a.AlertId,
                    name = a.Name,
                    enabled = a.Enabled,
                    event_source = a.EventSource,
                    message_id = a.MessageId,
                    severity = a.Severity,
                    database_name = a.DatabaseName,
                    event_description_keyword = a.EventDescriptionKeyword,
                    performance_condition = a.PerformanceCondition,
                    delay_between_responses_seconds = a.DelayBetweenResponses,
                    last_occurrence = FormatServerLocal(a.LastOccurrence),
                    last_response = FormatServerLocal(a.LastResponse),
                    occurrence_count = a.OccurrenceCount,
                    count_reset = FormatServerLocal(a.CountReset),
                    has_notification = a.HasNotification,
                    operator_count = a.OperatorCount,
                    response_job_id = a.ResponseJobId,
                    fired_in_window = a.FiredInWindow,
                }),
                note = "captured_at, as_of and oldest_capture_in_window are UTC. last_occurrence, last_response and count_reset are the monitored server's own clock, the way msdb stores them. fired_in_window compares occurrence_count between the first and last capture in the window, so a firing before the first capture is not seen. "
                    + "A severity or error with a disabled alert is reported as alert_disabled, not as missing.",
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_agent_alerts", ex);
        }
    }
}
