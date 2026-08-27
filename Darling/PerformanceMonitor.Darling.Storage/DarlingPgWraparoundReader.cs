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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads freeze headroom (<c>pg_wraparound_stats</c>) — the current distance to a write outage, per
/// database.
/// </summary>
public static class DarlingPgWraparoundReader
{
    public sealed record PgWraparoundRow(
        string DatabaseName,
        DateTime MeasuredAt,
        long FrozenXidAge,
        long MinMultiXidAge,
        long AutovacuumFreezeMaxAge,
        long AutovacuumMultixactFreezeMaxAge,
        double PctTowardEmergencyVacuum,
        double PctTowardWraparound,
        double PctTowardMultixactEmergency,
        double PctTowardMultixactWraparound,
        long XidsRemaining,
        long MultiXidsRemaining,
        bool AllowsConnections,
        long WindowPeakFrozenXidAge,
        long WindowPeakMinMultiXidAge);

    /// <summary>
    /// The LATEST reading per database, plus the window's peak for each counter.
    /// <para>Deliberately not an aggregate over the window the way the rate collectors are read. Freeze
    /// age is a level, not accumulated work: averaging it would blur the only number that matters, and
    /// summing it would be nonsense. The current value answers "how much time is left"; the window peak
    /// answers "did autovacuum actually claw it back, or has it only ever climbed" — a pair that
    /// distinguishes a healthy sawtooth from a monotonic march at a glance.</para>
    /// <para><c>DISTINCT ON</c> with the ordering below takes the newest row per database. The window
    /// maxima are computed before DISTINCT is applied, so they see every row in the window, not just the
    /// surviving one.</para>
    /// <para>$1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    public const string PgWraparoundSql = """
        SELECT DISTINCT ON (database_name)
            database_name,
            collection_time,
            frozen_xid_age,
            min_multixid_age,
            autovacuum_freeze_max_age,
            autovacuum_multixact_freeze_max_age,
            pct_toward_emergency_vacuum,
            pct_toward_wraparound,
            pct_toward_multixact_emergency,
            pct_toward_multixact_wraparound,
            xids_remaining,
            multixids_remaining,
            allows_connections,
            MAX(frozen_xid_age) OVER (PARTITION BY database_name) AS window_peak_frozen_xid_age,
            MAX(min_multixid_age) OVER (PARTITION BY database_name) AS window_peak_min_multixid_age
        FROM pg_wraparound_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY database_name, collection_time DESC
        """;

    public static async Task<List<PgWraparoundRow>> GetPgWraparoundAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgWraparoundRow>();
        await using var command = postgres.CreateCommand(PgWraparoundSql);
        command.Parameters.AddWithValue(serverId);
        /* Kind-Unspecified at the BIND, per the store's naive-UTC discipline: a Kind=Utc DateTime makes
           Npgsql infer timestamptz, and PostgreSQL then resolves the comparison against these naive
           timestamp columns by converting THEM at the store session's TimeZone - east of UTC every fresh
           row falls out of the window and the read silently returns nothing. Hidden by UTC-hosted test
           stores; found by the round-2 review. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgWraparoundRow(
                reader.GetString(0),
                reader.GetDateTime(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : reader.GetDouble(6),
                reader.IsDBNull(7) ? 0 : reader.GetDouble(7),
                reader.IsDBNull(8) ? 0 : reader.GetDouble(8),
                reader.IsDBNull(9) ? 0 : reader.GetDouble(9),
                reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                !reader.IsDBNull(12) && reader.GetBoolean(12),
                reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                reader.IsDBNull(14) ? 0 : reader.GetInt64(14)));
        }

        return rows;
    }
}
