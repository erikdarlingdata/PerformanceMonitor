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
///
/// <para><b>The reading carries the capacity gauge beside the raw CPU</b> (#3281), because the raw CPU is
/// percent of the capacity CURRENTLY ALLOCATED and the alert must threshold percent of the CONFIGURED
/// ceiling. Both come off the same row, so the alert compares a headroom figure and a CPU figure from the
/// same minute rather than joining two reads whose freshness could differ.</para>
/// </summary>
public static class DarlingPgCpuUtilizationReader
{
    /// <summary>
    /// How far back a reading may be and still count as current. Wider than the collector's own 5-minute
    /// cadence (<c>CollectorScheduleDefaults["pg_cpu_utilization"]</c>) to tolerate one missed cycle without
    /// the alert going silent, narrower than <see cref="DarlingPostgresAlertReadAdapter.Freshness"/>'s 2 hours
    /// because a stale CPU reading is a much weaker signal about "right now" than a wraparound age is — CPU
    /// moves in seconds, not days.
    ///
    /// <para><b>Public because the fleet card shares it</b> (#3267). <c>DarlingFleetReader</c>'s
    /// cross-server read answers the same question this one does — "where does this server's CPU stand
    /// right now" — and two surfaces answering that with different staleness windows is a surface drift of
    /// the #2473 kind: the High CPU alert would be evaluating a reading the card had already dropped, or
    /// the reverse. One constant, two consumers.</para>
    /// </summary>
    public static readonly TimeSpan Freshness = TimeSpan.FromMinutes(15);

    internal const string LatestCpuSql = """
        SELECT cpu_percent, sample_time, acu_utilization_percent, serverless_capacity_acu, max_configured_acu
        FROM pg_cpu_utilization
        WHERE server_id = $1
        AND   sample_time >= $2
        AND   cpu_percent IS NOT NULL
        ORDER BY sample_time DESC
        LIMIT 1
        """;

    /// <param name="CpuPercent"><c>os.cpuUtilization.total.avg</c> — percent of the capacity CURRENTLY
    /// ALLOCATED. Real, and not the saturation figure on a serverless instance class (#3281).</param>
    /// <param name="AcuUtilizationPercent">Percent of the CONFIGURED capacity ceiling in use — the figure
    /// the High CPU alert thresholds. Nullable, and a null must NOT fall back to
    /// <paramref name="CpuPercent"/>, which is a fraction of an allocation that moves.</param>
    /// <param name="ServerlessCapacityAcu">ACU allocated at this sample, so the alert can say "4 of 12"
    /// rather than only a percentage.</param>
    /// <param name="MaxConfiguredAcu">The configured ACU ceiling at this sample.</param>
    public sealed record CpuReading(
        double CpuPercent,
        DateTime SampleTimeUtc,
        double? AcuUtilizationPercent = null,
        double? ServerlessCapacityAcu = null,
        double? MaxConfiguredAcu = null);

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
            DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc),
            /* Each capacity column independently nullable: Performance Insights returns a data point with
               a null value for a period it has no sample for, and the ingestor stores that as NULL rather
               than 0 (a 0 here would read as measured headroom). */
            reader.IsDBNull(2) ? null : reader.GetDouble(2),
            reader.IsDBNull(3) ? null : reader.GetDouble(3),
            reader.IsDBNull(4) ? null : reader.GetDouble(4));
    }

    internal const string HistorySql = """
        SELECT sample_time, cpu_percent, acu_utilization_percent, serverless_capacity_acu, max_configured_acu
        FROM pg_cpu_utilization
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   cpu_percent IS NOT NULL
        ORDER BY sample_time
        """;

    /// <param name="CpuPercent">Percent of the capacity CURRENTLY ALLOCATED (#3281).</param>
    /// <param name="AcuUtilizationPercent">Percent of the CONFIGURED ceiling in use, or null where
    /// Performance Insights had no capacity sample for this minute.</param>
    /// <param name="ServerlessCapacityAcu">ACU allocated at this minute, or null.</param>
    /// <param name="MaxConfiguredAcu">The configured ACU ceiling at this minute, or null.</param>
    public sealed record CpuSample(
        DateTime SampleTimeUtc,
        double CpuPercent,
        double? AcuUtilizationPercent = null,
        double? ServerlessCapacityAcu = null,
        double? MaxConfiguredAcu = null);

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
                reader.GetDouble(1),
                reader.IsDBNull(2) ? null : reader.GetDouble(2),
                reader.IsDBNull(3) ? null : reader.GetDouble(3),
                reader.IsDBNull(4) ? null : reader.GetDouble(4)));
        }

        return samples;
    }
}
