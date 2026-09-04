/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One Alerts-tab row over <c>config_alert_log</c>, mirroring Lite's <c>AlertHistoryRow</c>
/// (Lite/Services/LocalDataService.AlertHistory.cs): the same metric-keyed value formatting
/// (#1134), the same email-vs-tray <see cref="StatusDisplay"/>, and the shared
/// <see cref="AlertMetricClassifier"/> for critical/warning/resolved row emphasis. Carries
/// <see cref="ServerId"/> + <see cref="ServerName"/> so the all-servers Alert History surface (W2a)
/// can show a Server column and key the dismiss write on (alert_time, server_id, metric_name).
/// Darling has no parquet archive tier, so there is no <c>Source</c>/<c>IsArchived</c> split — every
/// row is live and dismissable.
/// </summary>
public sealed class ViewerAlertRow
{
    /// <summary>Stored naive-UTC alert time.</summary>
    public required DateTime AlertTime { get; init; }

    /// <summary>The server the alert fired for (dismiss keys on it; the Server column shows the name).</summary>
    public int ServerId { get; init; }

    /// <summary>The alert's server display name (the Server column + mute-from-alert context).</summary>
    public string ServerName { get; init; } = "";

    public required string MetricName { get; init; }

    public required double CurrentValue { get; init; }

    public required double ThresholdValue { get; init; }

    public required bool AlertSent { get; init; }

    public required string NotificationType { get; init; }

    public string? SendError { get; init; }

    public required bool Muted { get; init; }

    public string? DetailText { get; init; }

    public string? ContextJson { get; init; }

    /// <summary>Stored naive-UTC; shown in the viewer machine's local time (the viewer convention).</summary>
    public string TimeLocal => ViewerTimeHelper.ForDisplay(AlertTime).ToString("yyyy-MM-dd HH:mm:ss");

    public string CurrentValueDisplay => AlertMetricClassifier.FormatHistoryValue(MetricName, CurrentValue);

    public string ThresholdValueDisplay => AlertMetricClassifier.FormatHistoryValue(MetricName, ThresholdValue);

    /// <summary>Email rows show send outcome; tray/other rows show shown-vs-delivered (Lite's mapping).</summary>
    public string StatusDisplay
    {
        get
        {
            if (NotificationType == "email")
            {
                return AlertSent ? "Sent" : (!string.IsNullOrEmpty(SendError) ? "Failed" : "Not sent");
            }

            return AlertSent ? "Delivered" : "Shown";
        }
    }

    public bool IsResolved => AlertMetricClassifier.IsResolution(MetricName);

    public bool IsCritical => AlertMetricClassifier.IsCritical(MetricName);

    public bool IsWarning => AlertMetricClassifier.IsWarning(MetricName);

}

public sealed partial class ViewerDataService
{
    /* The alert-history reads — Lite's GetAlertHistoryAsync query adapted to Darling's config_alert_log
       (no source column and no archive tier, so no v_config_alert_log union; Darling rows are always
       "live"). Same dismissed = FALSE filter, newest first, windowed on alert_time. Two shapes mirror
       Lite: one scoped to a server, one across ALL servers (the Alert History default). Both SELECT
       server_id + server_name so the grid's Server column and the dismiss key have them. */

    private const string AlertHistorySelectColumns = @"
    alert_time,
    server_id,
    server_name,
    metric_name,
    current_value,
    threshold_value,
    alert_sent,
    notification_type,
    send_error,
    muted,
    detail_text,
    context_json";

    /// <summary>Per-server read. $1 window start, $2 server_id, $3 limit (naive UTC / int / int).</summary>
    public const string AlertHistorySql = @"
SELECT" + AlertHistorySelectColumns + @"
FROM config_alert_log
WHERE alert_time >= $1
AND   server_id = $2
AND   dismissed = FALSE
ORDER BY alert_time DESC
LIMIT $3";

    /// <summary>All-servers read (the Alert History default). $1 window start, $2 limit (naive UTC / int).</summary>
    public const string AlertHistoryAllServersSql = @"
SELECT" + AlertHistorySelectColumns + @"
FROM config_alert_log
WHERE alert_time >= $1
AND   dismissed = FALSE
ORDER BY alert_time DESC
LIMIT $2";

    /// <summary>
    /// Recent alerts newest first, excluding dismissed rows — the Alert History tab's read. With no
    /// <paramref name="serverId"/> it aggregates ALL servers (the tab's default); with one it scopes to
    /// that server (the Server filter combo). Mirrors Lite's optional-serverId GetAlertHistoryAsync.
    /// </summary>
    public async Task<List<ViewerAlertRow>> GetAlertHistoryAsync(
        DateTime sinceUtc, int? serverId = null, int limit = 500, CancellationToken cancellationToken = default)
    {
        var rows = new List<ViewerAlertRow>();

        await using var command = _dataSource.CreateCommand(serverId.HasValue ? AlertHistorySql : AlertHistoryAllServersSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified),
        });
        if (serverId.HasValue)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId.Value });
        }
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerAlertRow
            {
                AlertTime = reader.GetDateTime(0),
                ServerId = reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                ServerName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                MetricName = reader.IsDBNull(3) ? "" : reader.GetString(3),
                CurrentValue = reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                ThresholdValue = reader.IsDBNull(5) ? 0 : reader.GetDouble(5),
                AlertSent = !reader.IsDBNull(6) && reader.GetBoolean(6),
                NotificationType = reader.IsDBNull(7) ? "" : reader.GetString(7),
                SendError = reader.IsDBNull(8) ? null : reader.GetString(8),
                Muted = !reader.IsDBNull(9) && reader.GetBoolean(9),
                DetailText = reader.IsDBNull(10) ? null : reader.GetString(10),
                ContextJson = reader.IsDBNull(11) ? null : reader.GetString(11),
            });
        }

        return rows;
    }

    /* ── Dismiss write path (W2a) ──────────────────────────────────────────────────────────────────
       Ports Lite's DismissAlertsAsync / DismissAllVisibleAlertsAsync UPDATE semantics (mark
       dismissed = TRUE so the row drops out of the default read). Lite wrapped the update in a
       transaction because it also wrote a dismissed_archive_alerts sidecar for rows that had aged into
       parquet; Darling is a single Postgres store with NO archive tier, so the sidecar is dropped and
       the two-phase live+sidecar dismiss collapses to one atomic UPDATE — no explicit transaction
       needed. Every match is keyed the same way Lite keyed it: (alert_time, server_id, metric_name). */

    /// <summary>
    /// Dismiss the given rows by (alert_time, server_id, metric_name). One set-based UPDATE keyed off
    /// unnested arrays (so the whole batch dismisses in a single round-trip). Already-dismissed rows are
    /// left alone by the <c>dismissed = FALSE</c> guard.
    /// </summary>
    public const string DismissAlertsSql = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  dismissed = FALSE
AND    (alert_time, server_id, metric_name) IN (
    SELECT t.alert_time, t.server_id, t.metric_name
    FROM   unnest($1::timestamp[], $2::integer[], $3::text[]) AS t(alert_time, server_id, metric_name)
)";

    /// <summary>Dismiss all visible rows in the window across every server. $1 window start.</summary>
    public const string DismissAllAlertsSql = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  alert_time >= $1
AND    dismissed = FALSE";

    /// <summary>Dismiss all visible rows in the window for one server. $1 window start, $2 server_id.</summary>
    public const string DismissAllAlertsForServerSql = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  alert_time >= $1
AND    server_id = $2
AND    dismissed = FALSE";

    /// <summary>
    /// Marks the given alerts dismissed. Returns how many live rows the UPDATE actually changed
    /// (already-dismissed or unknown rows count 0). See <see cref="DismissAlertsSql"/>.
    /// </summary>
    public async Task<int> DismissAlertsAsync(IReadOnlyList<ViewerAlertRow> alerts, CancellationToken cancellationToken = default)
    {
        if (alerts is null)
        {
            throw new ArgumentNullException(nameof(alerts));
        }

        if (alerts.Count == 0)
        {
            return 0;
        }

        var times = new DateTime[alerts.Count];
        var ids = new int[alerts.Count];
        var metrics = new string[alerts.Count];
        for (var i = 0; i < alerts.Count; i++)
        {
            times[i] = DateTime.SpecifyKind(alerts[i].AlertTime, DateTimeKind.Unspecified);
            ids[i] = alerts[i].ServerId;
            metrics[i] = alerts[i].MetricName;
        }

        await using var command = _dataSource.CreateCommand(DismissAlertsSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter { Value = times });
        command.Parameters.Add(new NpgsqlParameter { Value = ids });
        command.Parameters.Add(new NpgsqlParameter { Value = metrics });

        return await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>
    /// Marks every visible (non-dismissed) alert in the window dismissed — scoped to one server when
    /// <paramref name="serverId"/> is set, otherwise across all servers (matching the current filter).
    /// Returns the number of rows changed.
    /// </summary>
    public async Task<int> DismissAllVisibleAlertsAsync(
        DateTime sinceUtc, int? serverId = null, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(
            serverId.HasValue ? DismissAllAlertsForServerSql : DismissAllAlertsSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified),
        });
        if (serverId.HasValue)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId.Value });
        }

        return await ExecuteWriteAsync(command, cancellationToken);
    }
}
