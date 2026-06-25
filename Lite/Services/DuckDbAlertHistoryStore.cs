/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// DuckDB-backed <see cref="IAlertHistoryStore"/> over the <c>config_alert_log</c>
/// table. Wraps the persistence that previously lived directly inside
/// <see cref="EmailAlertService"/> (LogAlertAsync / GetLastEmailSentUtcAsync /
/// GetLastAlertTimeAsync) — verbatim SQL, same DuckDbInitializer + App.DatabasePath
/// fallback. The shared <c>string serverId</c> is parsed back to the DuckDB
/// <c>INT</c> column before binding the $1 parameter (§4.1).
/// </summary>
public sealed class DuckDbAlertHistoryStore : IAlertHistoryStore
{
    private readonly DuckDbInitializer? _duckDb;

    /* Failure tracking for louder logging — moved with LogAlertAsync. */
    private int _consecutiveLogFailures;

    public DuckDbAlertHistoryStore(DuckDbInitializer? duckDb = null)
    {
        _duckDb = duckDb;
    }

    /// <summary>
    /// Logs an alert to the config_alert_log table in DuckDB.
    /// Reuses the injected DuckDbInitializer instead of creating a new one each time.
    /// </summary>
    public async Task RecordAlertAsync(AlertHistoryRecord record)
    {
        /* Resolve the DuckDB current_value/threshold_value doubles from the
           optional numerics, falling back to parsing the display text — exactly
           the ?? fallback EmailAlertService.TrySendAlertEmailAsync used today. */
        var currentValue = record.NumericCurrentValue
            ?? (double.TryParse(record.CurrentValueText.TrimEnd('%'), out var cv) ? cv : 0);
        var thresholdValue = record.NumericThresholdValue
            ?? (double.TryParse(record.ThresholdValueText.TrimEnd('%'), out var tv) ? tv : 0);
        var serverId = int.TryParse(record.ServerId, out var sid) ? sid : 0;

        try
        {
            /* Use injected initializer, fall back to creating one from App.DatabasePath */
            var duckDb = _duckDb;
            if (duckDb == null)
            {
                var dbPath = App.DatabasePath;
                if (string.IsNullOrEmpty(dbPath)) return;
                duckDb = new DuckDbInitializer(dbPath);
            }

            using var writeLock = duckDb.AcquireWriteLock();
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = @"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, detail_text, context_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";

            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = DateTime.UtcNow });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = record.ServerName });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = record.MetricName });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = currentValue });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = thresholdValue });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = record.AlertSent });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = record.NotificationType });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = record.SendError ?? (object)DBNull.Value });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = record.Muted });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = record.DetailText ?? (object)DBNull.Value });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = record.ContextJson ?? (object)DBNull.Value });

            await command.ExecuteNonQueryAsync();

            /* Reset log failure counter on success */
            if (_consecutiveLogFailures > 0)
            {
                AppLogger.Info("EmailAlert", $"Alert logging recovered after {_consecutiveLogFailures} failure(s)");
            }
            _consecutiveLogFailures = 0;
        }
        catch (Exception ex)
        {
            _consecutiveLogFailures++;
            if (_consecutiveLogFailures <= 3)
            {
                AppLogger.Error("EmailAlert", $"Failed to log alert ({_consecutiveLogFailures}x): {ex.Message}");
            }
            else if (_consecutiveLogFailures % 50 == 0)
            {
                AppLogger.Error("EmailAlert", $"Alert logging STILL broken: {_consecutiveLogFailures} failures. Last: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Returns the UTC time the most recent alert email was successfully sent
    /// for this server/metric, read from config_alert_log — or null if none.
    /// Used to seed the in-memory cooldown after an app restart (#981). When
    /// <paramref name="dedupKey"/> is non-null (#1154), the result is additionally
    /// restricted to rows whose context_json carries that #1140 fingerprint.
    /// </summary>
    public async Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null)
    {
        var sid = int.TryParse(serverId, out var s) ? s : 0;
        try
        {
            /* Use injected initializer, fall back to creating one from App.DatabasePath */
            var duckDb = _duckDb;
            if (duckDb == null)
            {
                var dbPath = App.DatabasePath;
                if (string.IsNullOrEmpty(dbPath)) return null;
                duckDb = new DuckDbInitializer(dbPath);
            }

            using var readLock = duckDb.AcquireReadLock();
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            /* A successful email send is logged with a notification_type of
               'email' / 'email+webhook' and a null send_error — that mirrors
               exactly when the cooldown is stamped after SendEmailAsync.
               #1154: when a dedupKey is supplied, push the per-fingerprint filter into
               DuckDB via an anchored LIKE on the serialized "DedupKey":"<hex>" property
               (the hex key has no LIKE wildcards; NULL context_json rows fail the match). */
            command.CommandText = @"
SELECT MAX(alert_time)
FROM config_alert_log
WHERE server_id = $1
AND   metric_name = $2
AND   notification_type IN ('email', 'email+webhook')
AND   send_error IS NULL"
            + (dedupKey is null ? "" : "\nAND   context_json LIKE $3");
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = sid });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = metricName });
            if (dedupKey is not null)
                command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = "%\"DedupKey\":\"" + dedupKey + "\"%" });

            var result = await command.ExecuteScalarAsync();
            if (result == null || result == DBNull.Value) return null;

            /* alert_time is written as DateTime.UtcNow; tag it UTC so the kind
               is explicit (the cooldown subtraction is tick math regardless). */
            return DateTime.SpecifyKind(Convert.ToDateTime(result), DateTimeKind.Utc);
        }
        catch (Exception ex)
        {
            AppLogger.Error("EmailAlert", $"Could not read persisted alert cooldown: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Returns the UTC time the most recent alert webhook was successfully sent
    /// for this server/metric, read from config_alert_log — or null if none.
    /// Seeds the webhook cooldown after restart so a Teams/Slack alert posted
    /// shortly before a restart is not re-posted afterward (#1145, mirroring the
    /// email seed #981). When <paramref name="dedupKey"/> is non-null (#1154), the
    /// result is additionally restricted to rows whose context_json carries that
    /// #1140 fingerprint.
    /// </summary>
    public async Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null)
    {
        var sid = int.TryParse(serverId, out var s) ? s : 0;
        try
        {
            /* Use injected initializer, fall back to creating one from App.DatabasePath */
            var duckDb = _duckDb;
            if (duckDb == null)
            {
                var dbPath = App.DatabasePath;
                if (string.IsNullOrEmpty(dbPath)) return null;
                duckDb = new DuckDbInitializer(dbPath);
            }

            using var readLock = duckDb.AcquireReadLock();
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            /* A successful webhook send is logged with a notification_type of
               'webhook' / 'email+webhook' — those types are only ever written
               when WebhookSent is true, so the type alone implies success.
               send_error tracks the EMAIL channel, so it is NOT filtered on:
               an email-failed-but-webhook-sent row must still seed the cooldown.
               #1154: when a dedupKey is supplied, push the per-fingerprint filter into
               DuckDB via an anchored LIKE on the serialized "DedupKey":"<hex>" property. */
            command.CommandText = @"
SELECT MAX(alert_time)
FROM config_alert_log
WHERE server_id = $1
AND   metric_name = $2
AND   notification_type IN ('webhook', 'email+webhook')"
            + (dedupKey is null ? "" : "\nAND   context_json LIKE $3");
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = sid });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = metricName });
            if (dedupKey is not null)
                command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = "%\"DedupKey\":\"" + dedupKey + "\"%" });

            var result = await command.ExecuteScalarAsync();
            if (result == null || result == DBNull.Value) return null;

            /* alert_time is written as DateTime.UtcNow; tag it UTC so the kind
               is explicit (the cooldown subtraction is tick math regardless). */
            return DateTime.SpecifyKind(Convert.ToDateTime(result), DateTimeKind.Utc);
        }
        catch (Exception ex)
        {
            AppLogger.Error("WebhookAlert", $"Could not read persisted webhook cooldown: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Returns the UTC time of the most recent alert_log row for this
    /// (serverId, metricName), regardless of notification channel or
    /// delivery result. Used by <see cref="AnalysisNotificationService"/>
    /// to seed its per-finding cooldown across restarts — unlike the
    /// email cooldown (which filters to successful sends), the analysis
    /// cooldown is stamped unconditionally, so the persisted equivalent
    /// is the latest row for that metric_name, period.
    /// </summary>
    public async Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName)
    {
        var sid = int.TryParse(serverId, out var s) ? s : 0;
        try
        {
            var duckDb = _duckDb;
            if (duckDb == null)
            {
                var dbPath = App.DatabasePath;
                if (string.IsNullOrEmpty(dbPath)) return null;
                duckDb = new DuckDbInitializer(dbPath);
            }

            using var readLock = duckDb.AcquireReadLock();
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            /* metric_name embeds the first 8 chars of StoryPathHash via
               FindingMessageFormatter.MetricName, so a short-hash collision
               between two findings could seed one from the other's history.
               Acceptable: collision rate is ~sqrt(2^32) ≈ 65k unique patterns
               per server before a 50% chance, and the failure mode is suppress
               (not over-notify). */
            command.CommandText = @"
SELECT MAX(alert_time)
FROM config_alert_log
WHERE server_id = $1
AND   metric_name = $2";
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = sid });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = metricName });

            var result = await command.ExecuteScalarAsync();
            if (result == null || result == DBNull.Value) return null;

            return DateTime.SpecifyKind(Convert.ToDateTime(result), DateTimeKind.Utc);
        }
        catch (Exception ex)
        {
            AppLogger.Error("AnalysisNotify", $"Could not read persisted analysis cooldown: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Loads all persisted edge-trigger watermarks (#1145), one entry per
    /// (server_id, metric_name). The caller seeds its in-memory watermark dicts
    /// from these at startup, before the first alert sweep, so a restart does not
    /// reset the watermark to 0 and re-fire (and re-post a webhook for) events
    /// still lingering in the rolling lookback window.
    /// </summary>
    public async Task<List<(int ServerId, string MetricName, int Watermark)>> LoadEdgeTriggerWatermarksAsync()
    {
        var result = new List<(int, string, int)>();
        try
        {
            var duckDb = _duckDb;
            if (duckDb == null)
            {
                var dbPath = App.DatabasePath;
                if (string.IsNullOrEmpty(dbPath)) return result;
                duckDb = new DuckDbInitializer(dbPath);
            }

            using var readLock = duckDb.AcquireReadLock();
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            /* Count-based rows only (blocking/deadlock). The time-based failed-job rows live in the
               same table but set watermark_time (NULL here) and are loaded by
               LoadFailedJobWatermarksAsync, so they never bleed into the count seed. */
            command.CommandText = @"
SELECT server_id, metric_name, watermark
FROM config_edge_trigger_watermarks
WHERE watermark_time IS NULL";

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                result.Add((Convert.ToInt32(reader.GetValue(0)), reader.GetString(1), Convert.ToInt32(reader.GetValue(2))));
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("Alerts", $"Could not load edge-trigger watermarks: {ex.Message}");
        }
        return result;
    }

    /// <summary>
    /// Upserts one edge-trigger watermark (#1145). Called on-change only — the gate
    /// returns the same watermark on the vast majority of sweeps — so this is a
    /// low-frequency write that piggybacks on the existing alert-store write lock.
    /// </summary>
    public async Task SaveEdgeTriggerWatermarkAsync(int serverId, string metricName, int watermark)
    {
        try
        {
            var duckDb = _duckDb;
            if (duckDb == null)
            {
                var dbPath = App.DatabasePath;
                if (string.IsNullOrEmpty(dbPath)) return;
                duckDb = new DuckDbInitializer(dbPath);
            }

            using var writeLock = duckDb.AcquireWriteLock();
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            /* INSERT OR REPLACE upserts on the (server_id, metric_name) primary key —
               one stable row per server/metric, overwritten each time the watermark moves. */
            command.CommandText = @"
INSERT OR REPLACE INTO config_edge_trigger_watermarks (server_id, metric_name, watermark, updated_at)
VALUES ($1, $2, $3, $4)";
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = metricName });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = watermark });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = DateTime.UtcNow });

            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Error("Alerts", $"Could not persist edge-trigger watermark ({metricName}): {ex.Message}");
        }
    }

    /* The failed-Agent-job watermark shares the edge-trigger table but is time-based, not a count:
       it holds the newest already-alerted failure's server-local run time (stored in watermark_time,
       not the INTEGER watermark column). One reserved metric_name row per server. */
    private const string FailedJobWatermarkMetric = "Failed Agent Job";

    /// <summary>
    /// Loads the persisted failed-job watermarks (#1145 parity for the failed-job toast). The caller
    /// seeds <c>_lastAlertedFailedJobTime</c> from these at startup so a restart does not re-fire tray
    /// toasts for failures still inside the lookback window that the user already saw and dismissed.
    /// The value is the server-local run time of the newest already-alerted failure, returned in its
    /// native basis (NOT coerced to UTC) so it compares directly against <c>FailedJobInfo.RunDateTime</c>.
    /// </summary>
    public async Task<List<(int ServerId, DateTime Watermark)>> LoadFailedJobWatermarksAsync()
    {
        var result = new List<(int, DateTime)>();
        try
        {
            var duckDb = _duckDb;
            if (duckDb == null)
            {
                var dbPath = App.DatabasePath;
                if (string.IsNullOrEmpty(dbPath)) return result;
                duckDb = new DuckDbInitializer(dbPath);
            }

            using var readLock = duckDb.AcquireReadLock();
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = @"
SELECT server_id, watermark_time
FROM config_edge_trigger_watermarks
WHERE metric_name = $1
AND   watermark_time IS NOT NULL";
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = FailedJobWatermarkMetric });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                result.Add((Convert.ToInt32(reader.GetValue(0)), Convert.ToDateTime(reader.GetValue(1))));
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("Alerts", $"Could not load failed-job watermarks: {ex.Message}");
        }
        return result;
    }

    /// <summary>
    /// Upserts one failed-job watermark — the newest already-alerted failure's server-local run time.
    /// Called on-change only (when a failed-job toast fires), so it is a low-frequency write that
    /// piggybacks on the existing alert-store write lock, mirroring <see cref="SaveEdgeTriggerWatermarkAsync"/>.
    /// </summary>
    public async Task SaveFailedJobWatermarkAsync(int serverId, DateTime watermark)
    {
        try
        {
            var duckDb = _duckDb;
            if (duckDb == null)
            {
                var dbPath = App.DatabasePath;
                if (string.IsNullOrEmpty(dbPath)) return;
                duckDb = new DuckDbInitializer(dbPath);
            }

            using var writeLock = duckDb.AcquireWriteLock();
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            /* watermark (the INTEGER count column) is unused for the time-based failed-job row;
               it is non-nullable, so write 0. The meaningful value lives in watermark_time. */
            command.CommandText = @"
INSERT OR REPLACE INTO config_edge_trigger_watermarks (server_id, metric_name, watermark, watermark_time, updated_at)
VALUES ($1, $2, 0, $3, $4)";
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = FailedJobWatermarkMetric });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = watermark });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = DateTime.UtcNow });

            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Error("Alerts", $"Could not persist failed-job watermark: {ex.Message}");
        }
    }
}
