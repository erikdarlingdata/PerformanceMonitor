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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #7041: the process's one handle on the running alert evaluator for the report send. The worker owns the evaluator (it holds the
/// deliverer, the mute check and the report builder), and the MCP host runs in the same process, so the worker publishes the evaluator
/// here and the tool sends through it: the same pipeline the scheduled report uses, with no second copy of it. Registered once by
/// <c>DarlingProcessServices</c>.
/// </summary>
public sealed class DailyHealthReportHub
{
    private DarlingSelfAlertEvaluator? _evaluator;

    /// <summary>Set by the worker once the store is up; null before then and in a test-constructed host.</summary>
    internal DarlingSelfAlertEvaluator? Evaluator
    {
        get => Volatile.Read(ref _evaluator);
        set => Volatile.Write(ref _evaluator, value);
    }

    /// <summary>Builds the report for the period that ended at the most recent send time and sends it. Leaves the delivered-today stamp alone.</summary>
    public async Task<DailyHealthReportSendResult> SendNowAsync(CancellationToken cancellationToken)
    {
        var evaluator = Evaluator;
        return evaluator is null
            ? DailyHealthReportSendResult.Refused("not_ready", "The alert engine has not started yet. Try again in a minute.")
            : await evaluator.SendDailyHealthReportNowAsync(cancellationToken);
    }
}

/// <summary>What <c>send_daily_health_report_now</c> did: sent (with the period it covered and what the alert pipeline reported), or refused with a reason.</summary>
public sealed record DailyHealthReportSendResult(
    bool Sent, string? Reason, string? Message, DateTime? PeriodEndUtc, int ServerCount, string? Delivery)
{
    internal static DailyHealthReportSendResult Refused(string reason, string message) => new(false, reason, message, null, 0, null);
}

/// <summary>#7041: <c>send_daily_health_report_now</c>, the manual send of the Daily Health Report. A write tool (mcp.allowWrites), never offered to an investigation agent.</summary>
[McpServerToolType]
public sealed class DarlingMcpDailyHealthReportTools
{
    [McpServerTool(Name = "send_daily_health_report_now"), Description(
        "Builds the Daily Health Report for the 24 hours that ended at the most recent send time (daily_report.send_at in daily_report.time_zone, see get_alert_settings) " +
        "and sends it through the normal alert pipeline now: the notification routes for the metric 'Daily Health Report' choose the channels and recipients, " +
        "and a mute rule on that metric mutes it. It does NOT count as the day's scheduled report: the scheduled one still goes out at its time, so a recipient can get two. " +
        "It works while daily_report.enabled is off, and is refused while the alert master switch is off. The answer says what the alert pipeline reported " +
        "(sent, muted, unconfigured when no channel is set up, or failed). Needs mcp.allowWrites; never offered to an investigation agent.")]
    public static async Task<string> SendDailyHealthReportNow(
        DailyHealthReportHub? hub = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        const string tool = "send_daily_health_report_now";
        try
        {
            var result = hub is null
                ? DailyHealthReportSendResult.Refused("not_ready", "The report sender is not available in this host.")
                : await hub.SendNowAsync(cancellationToken);

            if (!result.Sent)
            {
                return new JsonObject
                {
                    ["status"] = "refused",
                    ["tool"] = tool,
                    ["reason"] = result.Reason,
                    ["message"] = result.Message + " Nothing was sent.",
                }.ToJsonString();
            }

            logger?.LogWarning("MCP {Tool} sent the daily health report, delivery {Delivery}, caller {Caller}", tool, result.Delivery, ReadCaller.WriteIdentity());
            return new JsonObject
            {
                ["status"] = "sent",
                ["tool"] = tool,
                ["what_changed"] = "The Daily Health Report was built and handed to the alert pipeline. No setting changed and the scheduled report's delivered-today record was not touched.",
                ["period_end_utc"] = result.PeriodEndUtc!.Value.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture),
                ["servers_covered"] = result.ServerCount,
                ["delivery"] = result.Delivery,
                ["undo"] = "None: a sent report cannot be recalled.",
                ["check"] = "get_alert_history",
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(tool, ex);
        }
    }
}
