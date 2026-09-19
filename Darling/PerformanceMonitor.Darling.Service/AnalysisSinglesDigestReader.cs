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
using NpgsqlTypes;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The store read behind the Analysis Singles Digest (#3712): every alert-history row the corroboration gate
/// routed to the digest inside a span. The ledger is the source rather than <c>analysis_findings</c>, on
/// purpose — a ledger row exists for exactly the findings the gate DECIDED about (notify-worthy, and
/// uncorroborated), one per fresh-or-worsening story rather than one per analysis cycle, and it already
/// carries the two things the digest owes the reader: the reason (<see cref="AlertContext.Routing"/>) and the
/// dedup fingerprints a page would have carried (<see cref="AlertContext.Incidents"/>). The findings table
/// has neither, and would make the digest re-derive a decision the write path already recorded.
///
/// <para>Discriminated on <c>notification_type = 'digest'</c> (<see cref="AlertDelivery.ChannelDigest"/>),
/// the disposition the two finding senders write for this route and nothing else writes — so the predicate
/// is the route, not a name pattern over <c>metric_name</c>. Schema-qualified like the other self-alert
/// readers, for the scratch-database live test that carries no search path.</para>
/// </summary>
public static class AnalysisSinglesDigestReader
{
    /// <summary>One digest-routed row: when, on which server, which finding (its metric name embeds the story's
    /// category and short hash — the key a page would have carried), at what severity, with the context the
    /// gate persisted (routing reason, dedup fingerprints).</summary>
    public sealed record DigestRoutedRow(
        DateTime AlertTime, int ServerId, string ServerName, string MetricName, double Severity, string? ContextJson);

    /// <summary>
    /// The span read. Ordered oldest-first so the extraction's "last seen" per story is the LAST row it
    /// meets; the extraction dedups (server, metric) pairs itself, so a story re-recorded after a restart
    /// counts once.
    /// </summary>
    internal const string DigestRoutedSql = @"
SELECT alert_time, server_id, server_name, metric_name, current_value, context_json
FROM config.config_alert_log
WHERE alert_time >= $1
AND   alert_time <  $2
AND   notification_type = $3
ORDER BY alert_time";

    /// <summary>The alert pass's own deadline, because this read runs inside the hourly self-metrics tick beside
    /// the collector-cost and rollup reads, which take the same one.</summary>
    internal const int CommandTimeoutSeconds = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds;

    /// <summary>
    /// Every digest-routed row with <c>alert_time</c> in [<paramref name="sinceUtc"/>, <paramref name="untilUtc"/>).
    /// May throw — the evaluator isolates and counts the failure, the FleetSweepStore posture: the caller is
    /// the one place that knows a failed read means "skip the tick without consuming the interval".
    /// </summary>
    public static async Task<List<DigestRoutedRow>> GetDigestRoutedRowsAsync(
        NpgsqlDataSource postgres, DateTime sinceUtc, DateTime untilUtc, CancellationToken cancellationToken)
    {
        if (postgres is null)
        {
            throw new ArgumentNullException(nameof(postgres));
        }

        var rows = new List<DigestRoutedRow>();

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(DigestRoutedSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        /* alert_time is written as naive UTC (the store's convention); bind as timestamp without time zone so
           the comparison is against the same clock the rows were stamped from. */
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.Timestamp
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(untilUtc, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.Timestamp
        });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = AlertDelivery.ChannelDigest });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new DigestRoutedRow(
                DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(5) ? null : reader.GetString(5)));
        }

        return rows;
    }
}
