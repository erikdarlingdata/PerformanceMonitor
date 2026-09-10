/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Postgres-backed <see cref="IAlertHistoryStore"/> over the V3 <c>config_alert_log</c> table —
/// Lite's <c>DuckDbAlertHistoryStore</c> ported query-for-query (same columns, same numeric
/// display-text fallback parse, same cooldown-seed filters including the #1154 anchored
/// context_json LIKE on the #1140 dedup fingerprint). RecordAlertAsync is failure-isolated with
/// Lite's escalating log cadence (first 3 failures, then every 50th) so a down store can't spam
/// the service log every sweep; the seed reads degrade to null. <c>alert_time</c> is stamped
/// naive-UTC at record time and tagged Utc on read, exactly like Lite's UtcNow discipline.
/// </summary>
public sealed class PgAlertHistoryStore : IAlertHistoryStore
{
    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;

    /* Failure tracking for louder logging — Lite's _consecutiveLogFailures cadence. */
    private int _consecutiveLogFailures;

    public PgAlertHistoryStore(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
    }

    public async Task RecordAlertAsync(AlertHistoryRecord record)
    {
        if (record is null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        /* Resolve the current_value/threshold_value doubles from the optional numerics, falling back
           to the display text (#1830: the old TrimEnd('%') parse failed on any decorated value and
           silently stored 0 for every High CPU row). Shared with Lite's DuckDbAlertHistoryStore
           rather than written out twice — see AlertValueParser.ResolveStoredValue for why that
           duplication was the drift risk #1881 turned on. */
        var currentValue = AlertValueParser.ResolveStoredValue(
            record.NumericCurrentValue, record.CurrentValueText);
        var thresholdValue = AlertValueParser.ResolveStoredValue(
            record.NumericThresholdValue, record.ThresholdValueText);
        var serverId = int.TryParse(record.ServerId, out var sid) ? sid : 0;

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(@"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, detail_text, context_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };

            command.Parameters.AddWithValue(NaiveUtcNow());
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(record.ServerName);
            command.Parameters.AddWithValue(record.MetricName);
            command.Parameters.AddWithValue(currentValue);
            command.Parameters.AddWithValue(thresholdValue);
            command.Parameters.AddWithValue(record.AlertSent);
            command.Parameters.AddWithValue(record.NotificationType);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)record.SendError ?? DBNull.Value });
            command.Parameters.AddWithValue(record.Muted);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)record.DetailText ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)record.ContextJson ?? DBNull.Value });

            await command.ExecuteNonQueryAsync();

            if (_consecutiveLogFailures > 0)
            {
                _logger?.LogInformation("Alert logging recovered after {Failures} failure(s)", _consecutiveLogFailures);
            }
            _consecutiveLogFailures = 0;
        }
        catch (Exception ex)
        {
            _consecutiveLogFailures++;
            if (_consecutiveLogFailures <= 3)
            {
                _logger?.LogError("Failed to log alert ({Failures}x): {Message}", _consecutiveLogFailures, ex.Message);
            }
            else if (_consecutiveLogFailures % 50 == 0)
            {
                _logger?.LogError("Alert logging STILL broken: {Failures} failures. Last: {Message}", _consecutiveLogFailures, ex.Message);
            }
        }
    }

    public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
        /* A successful email send is logged with notification_type 'email'/'email+webhook' and a
           null send_error — exactly when Lite stamps the email cooldown (#981). */
        ReadMaxAlertTimeAsync(serverId, metricName, dedupKey, @"
AND   notification_type IN ('email', 'email+webhook')
AND   send_error IS NULL", "EmailAlert");

    public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
        /* 'webhook'/'email+webhook' types are only written on a successful post, so the type
           alone implies success; send_error tracks the EMAIL channel and is NOT filtered (#1145). */
        ReadMaxAlertTimeAsync(serverId, metricName, dedupKey, @"
AND   notification_type IN ('webhook', 'email+webhook')", "WebhookAlert");

    public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
        /* Unfiltered — any channel/result (the analysis cooldown is stamped unconditionally). #2716
           reuses the dedupKey filter to seed Postgres Tier-0-predictor cooldowns across a restart. */
        ReadMaxAlertTimeAsync(serverId, metricName, dedupKey, extraFilter: "", logScope: "AnalysisNotify");

    private async Task<DateTime?> ReadMaxAlertTimeAsync(
        string serverId, string metricName, string? dedupKey, string extraFilter, string logScope)
    {
        var sid = int.TryParse(serverId, out var s) ? s : 0;
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(@"
SELECT MAX(alert_time)
FROM config_alert_log
WHERE server_id = $1
AND   metric_name = $2" + extraFilter
                /* #1154: the per-fingerprint filter — Lite's anchored LIKE on the serialized
                   "DedupKey":"<value>" property. #2716 added a caller whose dedupKey is a raw,
                   human-readable name rather than a hash, so the pattern is built (and any LIKE/JSON
                   special characters escaped) by AlertContextSerializer.BuildDedupKeyLikePattern
                   rather than hand-concatenated here — see its doc comment for why. NULL context_json
                   rows fail the match either way. */
                + (dedupKey is null ? "" : "\nAND   context_json LIKE $3 ESCAPE '\\'"), connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
            command.Parameters.AddWithValue(sid);
            command.Parameters.AddWithValue(metricName);
            if (dedupKey is not null)
            {
                command.Parameters.AddWithValue(AlertContextSerializer.BuildDedupKeyLikePattern(dedupKey));
            }

            var result = await command.ExecuteScalarAsync();
            if (result == null || result == DBNull.Value)
            {
                return null;
            }

            /* alert_time is written as naive UTC; tag it UTC so the kind is explicit. */
            return DateTime.SpecifyKind(Convert.ToDateTime(result, CultureInfo.InvariantCulture), DateTimeKind.Utc);
        }
        catch (Exception ex)
        {
            _logger?.LogError("{Scope}: could not read persisted alert cooldown: {Message}", logScope, ex.Message);
            return null;
        }
    }

    /// <summary>Naive-UTC now, Kind-Unspecified — the product's PG timestamp discipline.</summary>
    private static DateTime NaiveUtcNow() =>
        DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
}
