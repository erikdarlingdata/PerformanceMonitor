/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The High CPU alert's read side (#2719) — the LATEST <c>pg_cpu_utilization</c> reading for a server, within
/// a freshness bound. A level, not an aggregate: the question the alert asks is "where does CPU stand right
/// now", matching <see cref="DarlingPostgresAlertReadAdapter"/>'s own "latest reading per subject" convention
/// for the three Tier 0 predictors.
/// </summary>
public static class DarlingPgCpuUtilizationReader
{
    /// <summary>
    /// How far back a reading may be and still count as current. Wider than the collector's own 5-minute
    /// cadence (<c>CollectorScheduleDefaults["pg_cpu_utilization"]</c>) to tolerate one missed cycle without
    /// the alert going silent, narrower than <see cref="DarlingPostgresAlertReadAdapter.Freshness"/>'s 2 hours
    /// because a stale CPU reading is a much weaker signal about "right now" than a wraparound age is — CPU
    /// moves in seconds, not days.
    /// </summary>
    internal static readonly TimeSpan Freshness = TimeSpan.FromMinutes(15);

    internal const string LatestCpuSql = """
        SELECT cpu_percent, sample_time
        FROM pg_cpu_utilization
        WHERE server_id = $1
        AND   sample_time >= $2
        AND   cpu_percent IS NOT NULL
        ORDER BY sample_time DESC
        LIMIT 1
        """;

    public sealed record CpuReading(double CpuPercent, DateTime SampleTimeUtc);

    public static async Task<CpuReading?> GetLatestAsync(
        NpgsqlDataSource postgres, int serverId, DateTime nowUtc, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        await using var command = postgres.CreateCommand(LatestCpuSql);
        command.Parameters.AddWithValue(serverId);
        /* Naive UTC at the bind, matching every other PG store comparison against the naive `timestamp`
           columns — see DarlingPgSessionStatesReader's identical comment for why Kind=Utc would silently
           shift this. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(nowUtc - Freshness, DateTimeKind.Unspecified));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new CpuReading(
            reader.GetDouble(0),
            DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc));
    }
}
