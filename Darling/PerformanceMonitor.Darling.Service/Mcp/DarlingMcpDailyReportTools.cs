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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Reports;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The Daily Health Report read (#7041): <c>get_daily_health_report</c> builds the same report the scheduled send delivers, over the
/// same <see cref="DailyHealthReportBuilder"/>, so an agent and the inbox cannot disagree about what a day looked like. It sends nothing.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpDailyReportTools
{
    [McpServerTool(Name = "get_daily_health_report"), Description(
        "Builds the Daily Health Report for one day from stored data and returns it as text plus structured sections: the fleet counts, " +
        "then each server worst first with its alerts, failed Agent jobs, overdue backups and CHECKDB, low disk and collection problems. " +
        "Reads the store only and sends nothing. Optional date (yyyy-MM-dd, in the report's time zone) names the day; the default is the " +
        "latest day whose period has ended.<<GUIDE>>" +
        "Builds the Daily Health Report the scheduler sends (daily_report in get_alert_settings holds the send time, time zone and title), " +
        "without sending it. The period is the 24 hours that end at that date's send time in the report's time zone; date (yyyy-MM-dd) picks the " +
        "day and the default is the latest period that has already ended, so a date whose send time is still ahead is refused. The result has " +
        "subject, text (the short version Slack, Teams and the webhook receive), period_start and period_end (UTC), a summary of server counts " +
        "by band and alerts fired, and servers worst first. Each server carries band, reasons, alerts (count per metric), failed_jobs (Agent job " +
        "outcomes, SQL Server only), database_protection (databases past the full backup, log backup or CHECKDB bar, SQL Server only), low_disk " +
        "(volumes under the low-disk alert's bars at the newest snapshot in the period, SQL Server only) and collection_problems (the server " +
        "offline, or failing collectors). A section with nothing in it is an empty list, and a server with every list empty is healthy with " +
        "nothing to report. The band, alert, job and disk sections are the chosen day's, but collection_problems and database_protection are " +
        "read as the store holds them now. Stored read.")]
    public static async Task<string> GetDailyHealthReport(
        NpgsqlDataSource postgres,
        [Description("Local date in the report's time zone, yyyy-MM-dd. Default: the latest day whose period has ended.")] string? date = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var config = await DarlingAlertReader.GetAlertConfigurationAsync(postgres, cancellationToken);
            var sendAt = DailyHealthReportSettings.ResolveSendAt(config.Settings?.DailyReportSendAt);
            var zone = DailyHealthReportSettings.ResolveZone(config.Settings?.DailyReportTimeZone);
            var nowUtc = DateTime.UtcNow;

            DateOnly day;
            if (string.IsNullOrWhiteSpace(date))
            {
                day = DailyHealthReportPeriod.LatestEndedDate(nowUtc, sendAt, zone);
            }
            else if (!DateOnly.TryParseExact(date.Trim(), "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out day))
            {
                return McpHelpers.Refusal("date", $"Invalid date '{date}'. Expected yyyy-MM-dd, a local date in the report's time zone.");
            }

            var periodEnd = DailyHealthReportPeriod.EndUtcFor(day, sendAt, zone);
            if (periodEnd > nowUtc)
            {
                return McpHelpers.Refusal(
                    "date",
                    $"The period for {day:yyyy-MM-dd} ends at {periodEnd:yyyy-MM-dd HH:mm} UTC, which has not happened yet. " +
                    "Omit date for the latest period that has ended.");
            }

            var data = await new DailyHealthReportBuilder(postgres).BuildDataAsync(periodEnd, cancellationToken);
            var rendered = DailyHealthReportBuilder.Render(data);

            var node = new JsonObject
            {
                ["title"] = data.Title,
                ["subject"] = rendered.Subject,
                ["local_date"] = data.LocalDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                ["time_zone"] = data.TimeZoneId,
                ["period_start"] = data.PeriodStartUtc,
                ["period_end"] = data.PeriodEndUtc,
                ["summary"] = new JsonObject
                {
                    ["servers"] = data.Servers.Count,
                    ["healthy_count"] = data.HealthyCount,
                    ["warning_count"] = data.WarningCount,
                    ["critical_count"] = data.CriticalCount,
                    ["offline_count"] = data.OfflineCount,
                    ["alerts_fired"] = data.AlertsFired,
                },
                ["text"] = rendered.Text,
                ["servers"] = new JsonArray(data.Servers.Select(ServerNode).ToArray<JsonNode?>()),
            };

            if (data.Servers.Count == 0)
            {
                node["status"] = "empty";
                node["message"] = "No servers are registered yet, so the report has nothing to cover.";
            }

            return node.ToJsonString(new JsonSerializerOptions { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping });
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_daily_health_report", ex);
        }
    }

    private static JsonObject ServerNode(DailyHealthServerReport s) => new()
    {
        ["server_id"] = s.ServerId,
        ["server_name"] = s.Name,
        ["engine"] = s.IsPostgres ? "postgresql" : "sql_server",
        ["band"] = s.Band,
        ["reasons"] = new JsonArray(s.Reasons.Select(r => JsonValue.Create(r)).ToArray<JsonNode?>()),
        ["alerts"] = new JsonArray(s.Alerts.Select(a => (JsonNode?)new JsonObject { ["metric"] = a.Metric, ["count"] = a.Count }).ToArray()),
        ["failed_jobs"] = new JsonArray(s.FailedJobs.Select(j => (JsonNode?)new JsonObject
        {
            ["job_name"] = j.JobName,
            ["failures"] = j.Failures,
            ["last_failure"] = j.LastFailureUtc,
            ["message"] = j.Message,
        }).ToArray()),
        ["database_protection"] = new JsonArray(s.Protection.Select(p => (JsonNode?)new JsonObject
        {
            ["kind"] = p.Kind,
            ["database"] = p.Database,
            ["text"] = p.Text,
        }).ToArray()),
        ["low_disk"] = new JsonArray(s.LowDisk.Select(d => (JsonNode?)new JsonObject
        {
            ["volume"] = d.Volume,
            ["free_gb"] = Math.Round(d.FreeGb, 1),
            ["total_gb"] = Math.Round(d.TotalGb, 1),
            ["free_percent"] = Math.Round(d.FreePercent, 1),
        }).ToArray()),
        ["collection_problems"] = new JsonArray(s.CollectionProblems.Select(c => JsonValue.Create(c)).ToArray<JsonNode?>()),
    };
}
