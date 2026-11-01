/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Postgres-backed <see cref="IAlertHistoryStore"/> over the V3 <c>config_alert_log</c> table —
/// Lite's <c>DuckDbAlertHistoryStore</c> ported query-for-query (same columns, same numeric
/// display-text fallback parse, same cooldown-seed filters including the #1154 anchored
/// context_json LIKE on the #1140 dedup fingerprint). RecordAlertAsync is failure-isolated with
/// Lite's escalating log cadence (first 3 failures, then every 50th) so a down store can't spam
/// the service log every sweep; the seed reads degrade to null. <c>alert_time</c> is stamped
/// naive-UTC at record time and tagged Utc on read, exactly like Lite's UtcNow discipline.
/// </summary>
public sealed class PgAlertHistoryStore : IAlertHistoryStore, IDeliveredFireReader
{
    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;
    private readonly int _commandTimeoutSeconds;

    /* Failure tracking for louder logging — Lite's _consecutiveLogFailures cadence. */
    private int _consecutiveLogFailures;

    /// <summary><paramref name="commandTimeoutSeconds"/> is the deadline of every statement this store runs; the caller
    /// states it (the alert pass passes its own 10 s, <c>DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds</c>).</summary>
    public PgAlertHistoryStore(NpgsqlDataSource postgres, int commandTimeoutSeconds, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _commandTimeoutSeconds = commandTimeoutSeconds;
        _logger = logger;
    }

    private const string InsertSql = @"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, detail_text, context_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";

    /// <summary>#5825: <see cref="InsertSql"/> plus the three resolution columns (V187), for a resolution row whose send
    /// had an outcome.</summary>
    private const string InsertResolutionSql = @"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, detail_text, context_json, resolution_sent_at, resolution_channels, resolution_outcome)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)";

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

        /* Unparseable keys (the self-alert family) collapse to the server_id 0 bucket — a write-only
           bucket the cooldown seed below refuses to read (#3456). One shared mapping with that read, so
           the two halves cannot drift back into answering the identity question differently. */
        var serverId = AlertHistoryServerIdentity.StorageId(record.ServerId);

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            /* #5825: a resolution that was sent, failed or muted also writes its three V187 columns. Every other row keeps
               the twelve-column insert it always had. */
            var resolution = record.ResolutionOutcome is not null;
            using var command = new NpgsqlCommand(resolution ? InsertResolutionSql : InsertSql, connection)
                { CommandTimeout = _commandTimeoutSeconds };

            /* #5632 Phase A: a stamped AlertTimeUtc is written as is, so the investigation link row that names this alert
               carries the same instant; otherwise the store's own clock, as before. */
            command.Parameters.AddWithValue(record.AlertTimeUtc is { } stamped
                ? DateTime.SpecifyKind(stamped, DateTimeKind.Unspecified)
                : NaiveUtcNow());
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(record.ServerName);
            command.Parameters.AddWithValue(record.MetricName);
            command.Parameters.AddWithValue(currentValue);
            command.Parameters.AddWithValue(thresholdValue);
            command.Parameters.AddWithValue(record.AlertSent);
            command.Parameters.AddWithValue(record.NotificationType);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)record.SendError ?? DBNull.Value });
            command.Parameters.AddWithValue(record.Muted);
            /* #4348/#5320 Layer 0b: the alert text and its context are judged where they are written, because the
               source rows an alert was built from may predate the collection-time filter. Clean text is the same
               instance, so the stored row is unchanged. */
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)DerivedStoreScrub.Text(record.DetailText) ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)DerivedStoreScrub.Json(record.ContextJson) ?? DBNull.Value });
            if (resolution)
            {
                command.Parameters.Add(new NpgsqlParameter
                {
                    NpgsqlDbType = NpgsqlDbType.Timestamp,
                    Value = record.ResolutionSentAtUtc is { } sentAt ? DateTime.SpecifyKind(sentAt, DateTimeKind.Unspecified) : DBNull.Value,
                });
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)record.ResolutionChannels ?? DBNull.Value });
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = record.ResolutionOutcome! });
            }

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

    /// <summary>
    /// Writes ONE custom-alert teardown resolution row (#3334) through the <c>SECURITY DEFINER</c>
    /// <c>config.record_custom_alert_resolution</c> function instead of a direct <c>INSERT INTO config_alert_log</c>.
    /// A caller-initiated rule delete runs on the least-privilege viewer/mcp pool, which may not INSERT the
    /// history table; the function (owned by the store owner, EXECUTE-granted to viewer/mcp) performs the
    /// privileged write while confining it to exactly a no-channel resolution row -- the fixed delivery-shape
    /// columns live in the function, so this passes only the four that vary. Maps the same way
    /// <see cref="RecordAlertAsync"/> writes a <c>BuildResolutionRecord</c> (title -> metric_name, message ->
    /// detail_text, zeroed/unmuted), but pinned to a no-channel row (<c>alert_sent</c> false,
    /// <c>notification_type</c> 'none') rather than a natural clear's 'tray'. NOT failure-isolated here: the sole caller
    /// (<c>CustomAlertEvaluator.WriteTeardownResolutionAsync</c>) already wraps it best-effort, and swallowing
    /// here would hide a missing grant/function behind a silent no-op -- exactly the class of bug #3334 is.
    /// </summary>
    public async Task RecordCustomAlertResolutionAsync(
        int serverId, string serverName, string metricName, string detailText, System.Threading.CancellationToken cancellationToken = default)
    {
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(
            "SELECT config.record_custom_alert_resolution($1, $2, $3, $4)", connection)
            { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = serverName });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = metricName });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = detailText });
        await command.ExecuteNonQueryAsync(cancellationToken);
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
        /* The #2716 Tier-0 seed: any channel/result, muted or not, because those live cooldowns are stamped
           unconditionally (a muted alert still consumes its cooldown), so the seed and the stamp agree on
           "any result". #3916 moved the ANALYSIS seed off this read to GetLastDeliveredPageUtcAsync below —
           do not add alert_sent here. The #3712 digest exclusion stays: a digest row is not a page, and none
           of the Tier-0 rows carry that disposition, so it is inert for them. Spelled as a parameter rather
           than inlined so the constant the writer uses is the constant the reader excludes. */
        ReadMaxAlertTimeAsync(serverId, metricName, dedupKey, DigestExclusionFilter, logScope: "AnalysisNotify");

    public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) =>
        /* #3916: the analysis #2054 hold is earned by a delivery, so its restart seed reads only rows that
           reached someone (alert_sent true = email and/or webhook delivered). A row that reached no one —
           the month-old undelivered row that held one production story silent for 35 days — seeds nothing.
           The digest exclusion is redundant by construction (a digest row is never Sent) and kept so the
           exclusion stays one symbol and pinned. */
        ReadMaxAlertTimeAsync(serverId, metricName, dedupKey: null, DeliveredPageFilter, logScope: "AnalysisNotify");

    /// <summary>
    /// #5715 part B: the tier of the newest fire row inside the window, null when a resolution row is as new or newer.
    /// See <see cref="IAlertHistoryStore.GetLastFiredTierAsync"/>. A failed read THROWS (no catch here): the engine
    /// logs it and retries on the next sweep, and "could not read" must not look like "no history". alert_time is
    /// microsecond-precision naive UTC; the window bound is sent the same way.
    /// </summary>
    public async Task<AlertSeverityLevel?> GetLastFiredTierAsync(
        string serverId, string metricName, string resolutionMetricName, DateTime sinceUtc, bool countMuted = true,
        AlertSeverityLevel? tierWhenRowCarriesNone = null, DateTime? untilUtc = null)
    {
        /* #7068: the one read of "the newest fire that no resolution closed" now serves the tier seed and the standing-incident seed. */
        var fire = await GetOpenFireAsync(serverId, metricName, new[] { resolutionMetricName }, sinceUtc, untilUtc, countMuted);
        return fire is null ? null : AlertContextSerializer.TryReadSeverity(fire.ContextJson) ?? tierWhenRowCarriesNone;
    }

    /// <summary>
    /// #7068: the newest fire row (inside the optional window, before the exclusive <paramref name="untilUtc"/>) that no
    /// resolution row closed. See <see cref="IAlertHistoryStore.GetOpenFireAsync"/>. A failed read THROWS (no catch here):
    /// the engine logs it, and "could not read" must not look like "no history". alert_time is microsecond-precision naive
    /// UTC; the bounds are sent the same way.
    /// </summary>
    public async Task<AlertHistoryFire?> GetOpenFireAsync(
        string serverId, string metricName, System.Collections.Generic.IReadOnlyList<string> resolutionMetricNames,
        DateTime? sinceUtc, DateTime? untilUtc, bool countMuted = true)
    {
        /* Same identity scope as GetLastEmailSentUtcAsync (#3456). */
        var sid = AlertHistoryServerIdentity.SeedScope(serverId);
        if (sid is null)
        {
            return null;
        }

        await using var connection = await _postgres.OpenConnectionAsync();

        DateTime? fireAt = null;
        double currentValue = 0;
        string? contextJson = null;
        using (var fireCommand = new NpgsqlCommand(@"
SELECT alert_time, current_value, context_json
FROM config_alert_log
WHERE server_id = $1
AND   metric_name = $2
AND   alert_time >= COALESCE($3, '-infinity'::timestamp)
AND   alert_time < COALESCE($4, 'infinity'::timestamp)"
            + AlertHistoryEscalationSeed.KeepUnlessEveryChannelFailedFilter
            + (countMuted ? "" : AlertHistoryEscalationSeed.ExcludeMutedFilter)
            + "\nORDER BY alert_time DESC\nLIMIT 1", connection) { CommandTimeout = _commandTimeoutSeconds })
        {
            fireCommand.Parameters.AddWithValue(sid.Value);
            fireCommand.Parameters.AddWithValue(metricName);
            fireCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = sinceUtc is { } since
                ? DateTime.SpecifyKind(AlertHistoryEscalationSeed.TruncateToMicroseconds(since), DateTimeKind.Unspecified) : DBNull.Value });
            /* #5715 round 1: exclusive, microsecond like the column; a row at or after the engine start is this process's own. */
            fireCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = untilUtc is { } until
                ? DateTime.SpecifyKind(AlertHistoryEscalationSeed.TruncateToMicroseconds(until), DateTimeKind.Unspecified) : DBNull.Value });
            await using var reader = await fireCommand.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                fireAt = DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc);
                currentValue = reader.IsDBNull(1) ? 0 : reader.GetDouble(1);
                contextJson = reader.IsDBNull(2) ? null : reader.GetString(2);
            }
        }

        if (fireAt is not { } fired)
        {
            return null;
        }

        using var resolutionCommand = new NpgsqlCommand(@"
SELECT MAX(alert_time)
FROM config_alert_log
WHERE server_id = $1
AND   metric_name = ANY($2)", connection) { CommandTimeout = _commandTimeoutSeconds };
        resolutionCommand.Parameters.AddWithValue(sid.Value);
        resolutionCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = System.Linq.Enumerable.ToArray(resolutionMetricNames) });
        var resolved = await resolutionCommand.ExecuteScalarAsync();
        DateTime? resolvedAt = resolved == null || resolved == DBNull.Value
            ? null
            : DateTime.SpecifyKind(Convert.ToDateTime(resolved, CultureInfo.InvariantCulture), DateTimeKind.Utc);

        return AlertHistoryEscalationSeed.IsOpen(fired, resolvedAt) ? new AlertHistoryFire(fired, currentValue, contextJson) : null;
    }

    /// <summary>#5825: the most delivered fire rows <see cref="GetOpenDeliveredFiresAsync"/> returns. An alert that
    /// repeats for days writes a row per send; the newest few name every channel the episode reached.</summary>
    public const int OpenDeliveredFireRowCap = 50;

    /// <summary>
    /// #5825: an open alert's delivered fire rows, so its clear can go where the fire went after a service restart emptied
    /// the deliverer's memory. "Open" is after the alert's last resolution row (a resolution row is stored under its title),
    /// so an episode that already cleared is never cleared twice. Delivered is <c>alert_sent</c>, not muted, not a digest;
    /// the caller reads each row's <c>Route</c> record for the channels that delivered. The first-fire time comes from
    /// every row in the episode, before the row cap. Seeks <c>idx_config_alert_log_time</c> (server_id, metric_name,
    /// alert_time) for both halves.
    /// </summary>
    public async Task<DeliveredFireHistory?> GetOpenDeliveredFiresAsync(
        string serverKey, string metricName, string resolutionTitle, CancellationToken cancellationToken = default)
    {
        /* Same identity scope as the cooldown seeds (#3456): a key that is not a server id has no per-server rows. */
        var sid = AlertHistoryServerIdentity.SeedScope(serverKey);
        if (sid is null)
        {
            return null;
        }

        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(@"
SELECT alert_time, current_value, detail_text, context_json, MIN(alert_time) OVER () AS first_fired
FROM config_alert_log
WHERE server_id = $1
AND   metric_name = $2
AND   alert_sent
AND   NOT muted" + DigestExclusionFilter + @"
AND   alert_time > COALESCE(
          (SELECT MAX(r.alert_time) FROM config_alert_log AS r WHERE r.server_id = $1 AND r.metric_name = $3),
          '-infinity'::timestamp)
ORDER BY alert_time DESC
LIMIT $4", connection) { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.AddWithValue(sid.Value);
        command.Parameters.AddWithValue(metricName);
        command.Parameters.AddWithValue(resolutionTitle);
        command.Parameters.AddWithValue(OpenDeliveredFireRowCap);
        var rows = new System.Collections.Generic.List<DeliveredFireRow>();
        DateTime? firstFired = null;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new DeliveredFireRow(
                DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                reader.IsDBNull(1) ? null : reader.GetDouble(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3)));
            firstFired ??= DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc);
        }

        return firstFired is { } first ? new DeliveredFireHistory(first, rows) : null;
    }

    /// <summary>The #3712 seed exclusion, as SQL text. A literal rather than a parameter for the reason the
    /// email/webhook filters above are: <see cref="AlertDelivery.ChannelDigest"/> is a compile-time constant
    /// of this codebase, and spelling it through the constant is what keeps the writer's value and the
    /// reader's exclusion one symbol. <c>internal</c> so the test can pin that the filter names the
    /// constant.</summary>
    internal const string DigestExclusionFilter = "\nAND   notification_type <> '" + AlertDelivery.ChannelDigest + "'";

    /// <summary>The #3916 delivered-page predicate: a delivered row, with the #3712 digest exclusion
    /// composed from its one symbol. <c>internal</c> so the test can pin its shape.</summary>
    internal const string DeliveredPageFilter = "\nAND   alert_sent" + DigestExclusionFilter;

    private async Task<DateTime?> ReadMaxAlertTimeAsync(
        string serverId, string metricName, string? dedupKey, string extraFilter, string logScope)
    {
        /* #3456: a key that is not an integer has no per-key rows to read — the write side collapses
           every such key into the server_id 0 bucket, so the old parse-to-0 fallback answered this key's
           question with MAX(alert_time) over EVERY collapsed key's rows: the fleet-wide last send of the
           metric, which is how one server's delivery came to throttle its siblings' first notices. No
           answer (null = no seed = first notice) is the correct one, and it fails toward posting. The
           invariant and the literal-0 exclusion live on AlertHistoryServerIdentity.SeedScope. */
        var sid = AlertHistoryServerIdentity.SeedScope(serverId);
        if (sid is null)
        {
            return null;
        }

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
                + (dedupKey is null ? "" : "\nAND   context_json LIKE $3 ESCAPE '\\'"), connection) { CommandTimeout = _commandTimeoutSeconds };
            command.Parameters.AddWithValue(sid.Value);
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
