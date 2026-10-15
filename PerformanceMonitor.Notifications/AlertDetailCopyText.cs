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
/// for display, so the copied text reads exactly like the window.
/// </summary>
public sealed record AlertDetailCopyHeader(
    string Time,
    string Server,
    string Metric,
    string CurrentValue,
    string Threshold,
    string Notification,
    string Status,
    bool Muted);

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
