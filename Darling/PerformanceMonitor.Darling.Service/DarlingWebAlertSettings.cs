/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The Daily Health Report settings write (#7041), mapped in <see cref="DarlingWebEndpoints"/> as
/// <c>PUT /api/alert-settings/daily-report</c>. The body is the <c>daily_report</c> group of <c>get_alert_settings</c>
/// (<c>enabled</c>, <c>send_at</c>, <c>time_zone</c>, <c>title</c>), and the write is <c>update_alert_settings</c>' own body
/// (<see cref="DarlingMcpAlertTools.UpdateAlertSettings"/>) run on the writes arm, so the ranges and the time zone check are the
/// tool's, not a second copy. A refused value answers 400 with the tool's sentence as it is.
/// </summary>
internal static class DarlingWebAlertSettings
{
    private static readonly string[] s_dailyReportKeys = ["enabled", "send_at", "time_zone", "title"];

    /// <summary>The 403 sentence a refused write answers: the web host runs on whatever login <c>postgres.webConnectionString</c> names.</summary>
    internal static string RefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("change alert settings", managedStore, "Change them in the Viewer app", adminWritesOff: adminWritesOff);

    internal static async Task<(int Status, JsonNode Body)> SaveDailyReportAsync(
        NpgsqlDataSource postgres, JsonObject body, bool managedStore = false, bool adminWritesOff = false)
    {
        var group = new JsonObject();
        foreach (var (key, value) in body)
        {
            if (Array.IndexOf(s_dailyReportKeys, key) < 0)
            {
                return (400, new JsonObject { ["error"] = "Unknown field: " + (key.Length > 40 ? key[..40] : key) + "." });
            }

            group[key] = value?.DeepClone();
        }

        if (group.Count == 0)
        {
            return (400, new JsonObject { ["error"] = "Send at least one of enabled, send_at, time_zone and title." });
        }

        var json = await DarlingMcpAlertTools.UpdateAlertSettings(postgres, new JsonObject { ["daily_report"] = group }.ToJsonString());
        var answer = JsonNode.Parse(json) as JsonObject;
        var status = answer?["status"]?.GetValue<string>();
        switch (status)
        {
            case "updated":
                return (200, answer!);
            case "invalid":
                return (400, new JsonObject { ["error"] = answer!["message"]?.GetValue<string>() ?? "That setting was refused." });
            default:
                var message = answer?["message"]?.GetValue<string>() ?? string.Empty;
                return message.Contains("permission denied", StringComparison.OrdinalIgnoreCase)
                    ? (403, new JsonObject { ["error"] = RefusedMessage(managedStore, adminWritesOff) })
                    : (500, new JsonObject { ["error"] = "The settings could not be saved." });
        }
    }
}
