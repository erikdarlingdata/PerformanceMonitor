/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The header of one alert-history row as the Alert Details window shows it (#5847): every value already formatted
/// for display, so the copied text reads exactly like the window. The three resolution values (#5825) are the
/// Darling Viewer's: a resolution row's send, copied only when <paramref name="ResolutionOutcome"/> is set, as the
/// window shows those rows only then. Lite has no resolution rows and leaves them null.
/// </summary>
public sealed record AlertDetailCopyHeader(
    string Time,
    string Server,
    string Metric,
    string CurrentValue,
    string Threshold,
    string Notification,
    string Status,
    bool Muted,
    string? ResolutionSentAt = null,
    string? ResolutionChannels = null,
    string? ResolutionOutcome = null);

/// <summary>
/// The plain text behind the Alert Details window's "Copy all details" button (#5847), shared by Lite and the Darling
/// Viewer so both copy the same shape: the header, one <c>Label: value</c> per line, then every detail item (its
/// heading, its prose or T-SQL, its fields), with a blank line between blocks. It reads the same persisted context
/// the window renders and falls back to the flat <c>detail_text</c> for rows that have none, so the copy holds what
/// the window holds. No WPF here: the windows only hand the result to the clipboard.
/// </summary>
public static class AlertDetailCopyText
{
    public static string Build(AlertDetailCopyHeader header, string? contextJson, string? detailText)
    {
        ArgumentNullException.ThrowIfNull(header);

        var text = new StringBuilder();
        AppendLine(text, "Time", header.Time);
        AppendLine(text, "Server", header.Server);
        AppendLine(text, "Metric", header.Metric);
        AppendLine(text, "Current Value", header.CurrentValue);
        AppendLine(text, "Threshold", header.Threshold);
        AppendLine(text, "Notification", header.Notification);
        AppendLine(text, "Status", header.Status);

        /* The same choice the window makes: the structured items when the context deserializes with any, else the
           flat detail_text. */
        var structured = AlertContextSerializer.TryDeserialize(contextJson, out var context) && context.Details.Count > 0;

        /* #5642's fire-time sample age, once, where both windows show it: the header row under Status. The flat text
           may already carry the line, and then it is not repeated. */
        var ageLine = AlertDetailText.SampleAgeLineFromJson(contextJson, structured ? null : detailText);
        if (ageLine is not null)
        {
            text.AppendLine(ageLine);
        }

        /* #5825: a resolution row's send, below the sample age, where the Viewer's window shows it. */
        if (header.ResolutionOutcome is not null)
        {
            AppendLine(text, "Resolution sent", header.ResolutionSentAt);
            AppendLine(text, "Resolution channels", header.ResolutionChannels);
            AppendLine(text, "Resolution outcome", header.ResolutionOutcome);
        }

        if (header.Muted)
        {
            text.AppendLine("This alert was muted by a mute rule");
        }

        if (structured)
        {
            foreach (var item in context.Details)
            {
                text.AppendLine();
                if (!string.IsNullOrWhiteSpace(item.Heading))
                {
                    text.AppendLine(item.Heading);
                }

                if (!string.IsNullOrWhiteSpace(item.Body))
                {
                    text.AppendLine(item.Body.TrimEnd());
                }

                foreach (var (label, value) in item.Fields)
                {
                    AppendLine(text, label, value);
                }
            }
        }
        else if (!string.IsNullOrWhiteSpace(detailText))
        {
            text.AppendLine();
            text.AppendLine(detailText.TrimEnd());
        }

        return text.ToString().TrimEnd() + Environment.NewLine;
    }

    private static void AppendLine(StringBuilder text, string label, string? value)
    {
        var name = label.TrimEnd().TrimEnd(':');
        text.Append(name).Append(": ").AppendLine(value ?? "");
    }
}

/// <summary>Which of a server's views holds the full record behind an alert (#5847).</summary>
public enum AlertChainView
{
    None,
    Blocking,
    Deadlocks
}

/// <summary>#6390 S6: the server tab an alert's metric belongs on, the Darling Viewer's Triage: the page that shows the signal the alert fired on.
/// The SQL Server names are tabs of a SQL Server tab; the Pg names are tabs of a PostgreSQL one.</summary>
public enum AlertServerTab
{
    None,
    Overview,
    WaitStats,
    Queries,
    Cpu,
    Memory,
    FileIo,
    TempDb,
    Blocking,
    RunningJobs,
    Configuration,
    SystemEvents,
    CollectionHealth,
    PgOverview,
    PgActivity,
    PgVacuum,
    PgWaits,
    PgReplication
}

/// <summary>
/// The Alert Details window's "open the full chain" link (#5847): which view a Blocking or Deadlocks alert opens, and
/// the window around the alert's time that view reads. The alert's statements are shortened when it is built, so the
/// view, which reads the full reports, is where the whole chain is.
/// </summary>
public static class AlertChainLink
{
    /// <summary>How far before the alert the view reads: the blocking and deadlock checks look back over the reports
    /// collected since their last pass, so the incident sits before the alert's time.</summary>
    public static readonly TimeSpan Before = TimeSpan.FromHours(1);

    /// <summary>How far after the alert the view reads, so reports collected just after it fired show too.</summary>
    public static readonly TimeSpan After = TimeSpan.FromMinutes(15);

    /// <summary>The view for an alert's metric name, or <see cref="AlertChainView.None"/> when no view holds more.</summary>
    public static AlertChainView ViewFor(string? metricName) => metricName switch
    {
        "Blocking Detected" => AlertChainView.Blocking,
        "Deadlocks Detected" => AlertChainView.Deadlocks,
        _ => AlertChainView.None
    };

    /// <summary>
    /// #6390 S6, the Darling Viewer's Triage link: the server tab that shows the signal an alert's metric fired on, opened over the alert's
    /// window (<see cref="WindowUtc"/>). Kept apart from <see cref="ViewFor"/> on purpose: Lite opens a Blocking or Deadlocks view from
    /// <see cref="ViewFor"/> and has no such link for any other metric, so widening that method would add links to Lite it cannot open.
    /// A metric with no server tab (the monitor's own store-wide self-alerts, external alerts, an unknown name) answers
    /// <see cref="AlertServerTab.None"/>.
    /// </summary>
    public static AlertServerTab TabFor(string? metricName, bool isPostgres)
    {
        if (isPostgres)
        {
            return metricName switch
            {
                "High CPU" or "Server Unreachable" or "Server Restored" => AlertServerTab.PgOverview,
                "Blocking Detected" or "Blocking Wait Time" or "Long-Running Query" => AlertServerTab.PgActivity,
                "Poison Wait" => AlertServerTab.PgWaits,
                "PostgreSQL Wraparound Risk" or "PostgreSQL Vacuum Horizon Blocked" => AlertServerTab.PgVacuum,
                "PostgreSQL Replication Slot Retention" => AlertServerTab.PgReplication,
                _ => AlertServerTab.None
            };
        }

        return metricName switch
        {
            "High CPU" or "Runnable Queue" => AlertServerTab.Cpu,
            "Blocking Detected" or "Blocking Wait Time" or "Deadlocks Detected" => AlertServerTab.Blocking,
            "Poison Wait" => AlertServerTab.WaitStats,
            "Long-Running Query" or "Client Timeouts" or "Forced Plan Failing" => AlertServerTab.Queries,
            "tempdb Space" or "Version Store (PVS)" => AlertServerTab.TempDb,
            "Volume Free Space" => AlertServerTab.FileIo,
            "Database File Growth" => AlertServerTab.SystemEvents,
            "Database State" => AlertServerTab.Configuration,
            "Failed Agent Job" or "Long-Running Job" or "Agent Not Running" => AlertServerTab.RunningJobs,
            "Collection Stopped" or "Collection Gap At Start" or "Collection Gaps In History" => AlertServerTab.CollectionHealth,
            "Server Unreachable" or "Server Restored" or "AG Failover" or "AG Replica Disconnected" or "AG Replica Reconnected"
                or "AG Sync Fell Behind" or "AG Database Suspended" => AlertServerTab.Overview,
            _ => AlertServerTab.None
        };
    }

    /// <summary>The Triage link's text for a metric that has a server tab on either engine, else null.</summary>
    public static string? TabLinkText(string? metricName) =>
        TabFor(metricName, false) != AlertServerTab.None || TabFor(metricName, true) != AlertServerTab.None
            ? "Open this server's tab at this time"
            : null;

    /// <summary>The naive-UTC window the view opens on, around the stored naive-UTC alert time.</summary>
    public static (DateTime FromUtc, DateTime ToUtc) WindowUtc(DateTime alertTimeUtc) =>
        (alertTimeUtc - Before, alertTimeUtc + After);

    /// <summary>The link's text, or null for <see cref="AlertChainView.None"/>.</summary>
    public static string? LinkText(AlertChainView view) => view switch
    {
        AlertChainView.Blocking => "Open the Blocking view at this time",
        AlertChainView.Deadlocks => "Open the Deadlocks view at this time",
        _ => null
    };
}
