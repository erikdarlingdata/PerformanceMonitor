/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The engine alerts an analysis finding attaches to instead of paging (#7084 part 4), the window that makes one
/// "open", and the reason text the held row carries. One definition for both stores' readers and the service.
/// <para>When one of these is open on the finding's server, a page-routed finding adds nothing a second page would
/// tell the operator: the engine alert already reached them. The finding is recorded as a digest row that names the
/// engine alert (its trailing <see cref="AlertContext.AttachedTo"/> member), and the engine alert's detail lists it.</para>
/// </summary>
public static class OpenEngineAlerts
{
    /// <summary>How far back an engine alert's delivered fire counts as open: a fire older than this has long since
    /// reached its reader, and the finding is news again.</summary>
    public static readonly TimeSpan Window = TimeSpan.FromMinutes(60);

    /// <summary>The attachable engine alerts and the title of the resolution row that closes each (a resolution row is
    /// stored under its title, not under the metric that fired). The titles are the strings the shared engine writes
    /// (EscalationSeed.CpuResolvedTitle and DeadlocksClearedTitle in PerformanceMonitor.Alerting, which this assembly cannot reference).</summary>
    public static readonly IReadOnlyList<(string Metric, string ResolutionTitle)> Families = new[]
    {
        ("High CPU", "CPU Resolved"),
        ("Blocking Detected", "Blocking Cleared"),
        ("Blocking Wait Time", "Blocking Wait Cleared"),
        ("Deadlocks Detected", "Deadlocks Cleared"),
    };

    /// <summary>True for a metric whose open fire a finding attaches to, and so for a row whose detail lists attached findings.</summary>
    public static bool IsAttachable(string? metricName)
    {
        if (metricName is null)
            return false;
        foreach (var (metric, _) in Families)
        {
            if (string.Equals(metric, metricName, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    /// <summary>The routing reason of a held finding row: <c>attached to High CPU fired 14:02</c> (UTC, like the row's own time).</summary>
    public static string Reason(AlertAttachedToDto attachedTo) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture,
            $"attached to {attachedTo.Metric} fired {attachedTo.AlertTimeUtc:HH:mm}");

    /// <summary>
    /// The statement behind an engine alert's "Attached findings" list: the digest rows of one server whose context carries an
    /// <c>AttachedTo</c> member, from the engine alert's fire time ($2) to the end of the window it could have been attached
    /// in ($3, from <see cref="AttachedUntil"/>). $1 is the server id. Narrow on purpose: the exact match on metric and time is
    /// <see cref="Names"/>, in code, so the SQL needs no JSON operator and the same text serves PostgreSQL and DuckDB.
    /// Seeks the alert log's (server_id, ..., alert_time) index and reads only the digest rows of one hour.
    /// </summary>
    public static string AttachedFindingsSql(string table) => @"
SELECT alert_time, metric_name, context_json
FROM " + table + @"
WHERE server_id = $1
AND   notification_type = '" + AlertDelivery.ChannelDigest + @"'
AND   alert_time >= $2
AND   alert_time <= $3
AND   context_json LIKE '%""AttachedTo"":{%'
ORDER BY alert_time";

    /// <summary>The last instant a finding could have attached to an engine alert fired at <paramref name="alertTimeUtc"/>:
    /// the window, plus a minute for the cycle that wrote the row.</summary>
    public static DateTime AttachedUntil(DateTime alertTimeUtc) => alertTimeUtc + Window + TimeSpan.FromMinutes(1);

    /// <summary>
    /// True when the finding row's own context names exactly this engine alert (same metric, same fire time to the
    /// microsecond the log stores). The readers of "attached findings" use it after a coarse SQL narrowing.
    /// </summary>
    public static bool Names(AlertAttachedToDto? attachedTo, string metric, DateTime alertTimeUtc) =>
        attachedTo is not null
        && string.Equals(attachedTo.Metric, metric, StringComparison.Ordinal)
        && AlertHistoryEscalationSeed.TruncateToMicroseconds(attachedTo.AlertTimeUtc).Ticks
            == AlertHistoryEscalationSeed.TruncateToMicroseconds(alertTimeUtc).Ticks;
}
