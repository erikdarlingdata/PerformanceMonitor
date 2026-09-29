/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// One-time heal of the daily rollup days an earlier hourly hole repair left short (#4716). Before the fix that
/// refreshes a daily's days whenever a repair closes the hourly range under them, a repaired hourly hole left
/// the day above it partial in every daily that already held a bucket for that day; a day older than the daily
/// policy's window is never refreshed again, so it stayed short, and it becomes the only copy when the hourly
/// ages out. The fix stops new cases. This checks the existing ones, once per daily.
///
/// <para><b>The work is in <see cref="TimescaleSupport.HealPartialDailyAsync"/>:</b> per daily, walk each
/// complete day older than the daily policy's window, compare <c>sum(sample_count)</c> on the daily against the
/// relation it is built on, and rebuild each day the daily holds fewer samples for, one at a time. This class
/// orders the dailies (<see cref="TimescaleSupport.PartialDailyHealOrder"/>: a daily built on another daily
/// after that daily), keeps the per-daily marker, and adds up the run.</para>
///
/// <para><b>The marker</b> reuses <c>collect.collector_state</c> under the fleet-sentinel <c>server_id</c>
/// (<see cref="DarlingObservability.FleetServerId"/>), the same store-wide one-shot shape
/// <see cref="PlanForceActionDetailScrub"/> uses — no migration. One row per daily: <c>state_key</c> is the
/// daily's view name, <c>state_value</c> is <see cref="DoneStateValue"/>. It is written for a daily ONLY when
/// its whole walk and every refresh in it succeeded, so a daily that failed, or a run that was cancelled, is
/// walked again on the next start; a daily whose marker is set is skipped without touching the rollups.</para>
///
/// <para><b>Failure is isolated per daily.</b> A daily that fails is logged once at Warning by the walk, counted
/// in <see cref="Summary.Failures"/>, and the next daily still runs. Cancellation propagates and writes no
/// marker.</para>
/// </summary>
public static class PartialDailyHeal
{
    /// <summary>The owner name in <c>collect.collector_state</c> — the <see cref="PlanForceActionDetailScrub"/>
    /// precedent, so no collector's declared-key read or per-database prune can reach it.</summary>
    public const string StateCollectorName = "partial_daily_heal";

    /// <summary>The marker's value: the issue that defined the heal. A different heal is a new key, not a new
    /// value on this one.</summary>
    public const string DoneStateValue = "4716";

    /// <summary>The marker read/write's deadline — one small keyed row.</summary>
    internal const int MarkerTimeoutSeconds = 30;

    private const string MarkerGetSql = @"
SELECT state_value
FROM collect.collector_state
WHERE server_id = $1 AND collector_name = $2 AND state_key = $3";

    private const string MarkerUpsertSql = @"
INSERT INTO collect.collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (server_id, collector_name, state_key)
DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = EXCLUDED.updated_at";

    /// <summary>What one run found and did, across every daily. <see cref="DailiesChecked"/> is every daily the
    /// run looked at; <see cref="DailiesAlreadyDone"/> those whose marker was already set. A day is
    /// <em>compared</em> when both the daily and its source hold rows for it, <em>partial</em> when the daily
    /// holds fewer samples, and <em>refreshed</em> when its rebuild succeeded; <see cref="DaysChained"/> is the
    /// days those rebuilds were chained on to the dailies built on the healed daily.</summary>
    public sealed record Summary(
        int DailiesChecked, int DailiesAlreadyDone, int DaysCompared, int DaysPartial, int DaysRefreshed, int DaysChained,
        int Failures, TimeSpan Elapsed);

    /// <summary>
    /// Runs the heal once on <paramref name="connection"/> (the hole repair's own connection, while its
    /// running flag is still set, so no other repair refreshes the same aggregate at the same time) and returns
    /// the tally — always, including the run that found nothing to do.
    /// </summary>
    public static async Task<Summary> RunAsync(
        NpgsqlConnection connection, ILogger? logger, DateTime utcNow, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var clock = Stopwatch.StartNew();
        var disclosure = new RefreshDisclosure(message => logger?.LogWarning("Partial-daily heal (#4716): {Message}", message));
        var dailiesChecked = 0;
        var alreadyDone = 0;
        var compared = 0;
        var partial = 0;
        var refreshed = 0;
        var chained = 0;
        var failures = 0;

        foreach (var (daily, source) in TimescaleSupport.PartialDailyHealOrder())
        {
            cancellationToken.ThrowIfCancellationRequested();
            dailiesChecked++;

            if (await ReadMarkerAsync(connection, daily, cancellationToken) == DoneStateValue)
            {
                alreadyDone++;
                continue;
            }

            var outcome = await TimescaleSupport.HealPartialDailyAsync(connection, logger, disclosure, daily, source, utcNow, cancellationToken);
            compared += outcome.DaysCompared;
            partial += outcome.DaysPartial;
            refreshed += outcome.DaysRefreshed;
            chained += outcome.DaysChained;

            if (!outcome.Available)
            {
                continue;
            }

            if (!outcome.Completed)
            {
                failures++;
                continue;
            }

            try
            {
                await WriteMarkerAsync(connection, daily, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failures++;
                logger?.LogWarning(
                    "Partial-daily heal (#4716): {Daily} was healed but its marker did not save ({ExceptionType}{SqlState}) — the next start walks it again.",
                    daily, ex.GetType().Name, ex is NpgsqlException { SqlState: { Length: > 0 } state } ? $", SQLSTATE {state}" : string.Empty);
            }
        }

        return new Summary(dailiesChecked, alreadyDone, compared, partial, refreshed, chained, failures, clock.Elapsed);
    }

    private static async Task<string?> ReadMarkerAsync(NpgsqlConnection connection, string daily, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(MarkerGetSql, connection) { CommandTimeout = MarkerTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = daily });

        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    private static async Task WriteMarkerAsync(NpgsqlConnection connection, string daily, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(MarkerUpsertSql, connection) { CommandTimeout = MarkerTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = daily });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DoneStateValue });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
        });

        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
