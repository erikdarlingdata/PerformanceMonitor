/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #8551: the web sidebar's alert badge read, <c>get_alert_badge_summary</c>. Web only, with no MCP tool behind it (see
/// <see cref="DarlingWebEndpoints.WebOnlyReadNames"/>): the badge used to count the newest 1,000 rows of
/// <c>get_alert_history</c> on every page load and every minute (about 1.8 MB, mostly detail text the badge never reads) and
/// undercounted whenever the cap cut the 24 hours short. This answers one entry per server that has something to show: how many
/// actionable alerts the last 24 hours hold, whether any fired Critical, and the newest alert's time. The grouping runs in the
/// store (<see cref="DarlingAlertReader.AlertBadgeGroupsSql"/>), so there is no row cap to hit and the reply is a few dozen bytes
/// per server.
///
/// <para>"Actionable" is the badge's own rule, moved here from <c>summarizeAlerts</c> in <c>viewer-local.js</c>: not muted, not
/// dismissed, and not a resolution (severity decided by <see cref="AlertHistoryRowSeverity.DescribeFired"/>, the rule
/// <c>get_alert_history</c> reports). The server grant is the same one <c>get_alert_history</c> reads, the ambient
/// <see cref="DarlingServerResolver.WebAllowedServerIds"/>, so a scoped reader gets no entry for a server outside the grant.</para>
/// </summary>
internal static class DarlingWebAlertBadgeSummary
{
    /// <summary>The badge's window, as the page always asked: the last 24 hours.</summary>
    internal const int WindowHours = 24;

    public static async Task<string> GetAlertBadgeSummary(NpgsqlDataSource postgres, DateTime? nowUtc, CancellationToken cancellationToken)
    {
        try
        {
            var end = nowUtc ?? DateTime.UtcNow;
            var groups = await DarlingAlertReader.GetAlertBadgeGroupsAsync(
                postgres, end.AddHours(-WindowHours), end, DarlingServerResolver.WebAllowedServerIds, cancellationToken);
            return BuildReply(groups);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_alert_badge_summary", ex);
        }
    }

    /// <summary>The reply for the store's groups: one entry per server with at least one actionable alert, by server id.</summary>
    internal static string BuildReply(IReadOnlyList<DarlingAlertReader.AlertBadgeGroup> groups)
    {
        var perServer = new SortedDictionary<int, (long Count, bool Critical, DateTime Latest)>();
        foreach (var g in groups)
        {
            var fired = g.FiredTier switch { 2 => AlertSeverityLevel.Critical, 1 => AlertSeverityLevel.Warning, _ => (AlertSeverityLevel?)null };
            var (severity, _) = AlertHistoryRowSeverity.DescribeFired(g.MetricName, fired);
            if (severity == "resolution") continue;

            perServer.TryGetValue(g.ServerId, out var s);
            perServer[g.ServerId] = (s.Count + g.Count, s.Critical || severity == "critical", g.LatestAlertTime > s.Latest ? g.LatestAlertTime : s.Latest);
        }

        var servers = new JsonArray(perServer.Select(kv => (JsonNode?)new JsonObject
        {
            ["server_id"] = kv.Key,
            ["count"] = kv.Value.Count,
            ["critical"] = kv.Value.Critical,
            ["latest_alert_time"] = kv.Value.Latest.ToString("o", CultureInfo.InvariantCulture),
        }).ToArray());

        return new JsonObject
        {
            ["status"] = "ok",
            ["window_hours"] = WindowHours,
            ["servers"] = servers,
        }.ToJsonString(McpHelpers.JsonOptions);
    }
}
