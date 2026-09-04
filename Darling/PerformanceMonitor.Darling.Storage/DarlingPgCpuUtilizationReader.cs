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
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
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

    internal const string HistorySql = """
        SELECT sample_time, cpu_percent
        FROM pg_cpu_utilization
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   cpu_percent IS NOT NULL
        ORDER BY sample_time
        """;

    public sealed record CpuSample(DateTime SampleTimeUtc, double CpuPercent);

    /// <summary>The served-read side (#2629/#2719's own fix) — every reading in a window, for
    /// <c>get_pg_cpu_utilization</c>. Windowed on <c>collection_time</c> (the ingestor's own cycle time)
    /// rather than <c>sample_time</c> (PI's data-point time), matching <see cref="DarlingDataReader.GetCpuUtilizationAsync"/>'s
    /// convention for the SQL Server twin: it is what every other windowed read here bounds on, and PI's
    /// data points arrive already time-ordered with no ring-buffer clock skew to correct for.</summary>
    public static async Task<System.Collections.Generic.List<CpuSample>> GetHistoryAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var samples = new System.Collections.Generic.List<CpuSample>();
        await using var command = postgres.CreateCommand(HistorySql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            samples.Add(new CpuSample(
                DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                reader.GetDouble(1)));
        }

        return samples;
    }
}
