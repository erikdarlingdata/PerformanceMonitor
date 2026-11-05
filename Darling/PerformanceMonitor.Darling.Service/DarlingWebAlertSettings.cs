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

        return await UpdateAsync(postgres, new JsonObject { ["daily_report"] = group }, managedStore, adminWritesOff);
    }

    /// <summary>The group and field each Database Protection age is written under, as <c>get_alert_settings</c> names them (#7611).</summary>
    private static readonly (string Group, string Field)[] s_databaseProtectionKeys =
    [
        ("backup_overdue", "threshold_hours"),
        ("log_backup_overdue", "threshold_minutes"),
        ("checkdb_overdue", "threshold_hours"),
    ];

    /// <summary>
    /// The Database Protection overdue ages (#7611), mapped as <c>PUT /api/alert-settings/database-protection</c>. The body is those
    /// groups of <c>get_alert_settings</c> cut to the age field (<c>{"backup_overdue":{"threshold_hours":168}, ...}</c>), and the write is
    /// <c>update_alert_settings</c>' own body, so the bounds are the tool's. The on/off boxes are not writable here.
    /// </summary>
    internal static async Task<(int Status, JsonNode Body)> SaveDatabaseProtectionAsync(
        NpgsqlDataSource postgres, JsonObject body, bool managedStore = false, bool adminWritesOff = false)
    {
        var payload = new JsonObject();
        foreach (var (groupName, groupValue) in body)
        {
            if (groupValue is not JsonObject fields || Array.FindIndex(s_databaseProtectionKeys, k => k.Group == groupName) < 0)
            {
                return (400, new JsonObject { ["error"] = "Unknown field: " + (groupName.Length > 40 ? groupName[..40] : groupName) + "." });
            }

            var group = new JsonObject();
            foreach (var (field, value) in fields)
            {
                if (Array.IndexOf(s_databaseProtectionKeys, (groupName, field)) < 0)
                {
                    return (400, new JsonObject { ["error"] = "Unknown field: " + groupName + "." + (field.Length > 40 ? field[..40] : field) + "." });
                }

                group[field] = value?.DeepClone();
            }

            if (group.Count > 0)
            {
                payload[groupName] = group;
            }
        }

        if (payload.Count == 0)
        {
            return (400, new JsonObject { ["error"] = "Send at least one of backup_overdue.threshold_hours, log_backup_overdue.threshold_minutes and checkdb_overdue.threshold_hours." });
        }

        return await UpdateAsync(postgres, payload, managedStore, adminWritesOff);
    }

    private static async Task<(int Status, JsonNode Body)> UpdateAsync(NpgsqlDataSource postgres, JsonObject payload, bool managedStore, bool adminWritesOff)
    {
        var json = await DarlingMcpAlertTools.UpdateAlertSettings(postgres, payload.ToJsonString());
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
