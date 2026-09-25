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
    /// <remarks>The three capacity members carry no DEFAULT, so every construction site has to state
    /// them. A defaulted null would let a future reader or test double produce a reading whose capacity is
    /// absent without saying so — and absent capacity is exactly the state the alert treats as
    /// "not measured", so it must never be reachable by omission.</remarks>
    public sealed record CpuReading(
        double CpuPercent,
        DateTime SampleTimeUtc,
        double? AcuUtilizationPercent,
        double? ServerlessCapacityAcu,
        double? MaxConfiguredAcu);

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

    /// <summary>
    /// The most samples one persistence-gate pass will consume (#3282). Performance Insights is queried at
    /// a 60-second period and <c>pg_cpu_utilization</c> is collected every five minutes, so a normal batch
    /// is about five samples; <see cref="Freshness"/> bounds how far back the pass looks, and this bounds
    /// how many rows it will carry out of that window if the collector caught up after a longer gap. The
    /// gate's counters saturate at their thresholds, so a larger batch could not change the outcome — this
    /// only stops one pass reading an unbounded list.
    /// </summary>
    public const int GateBatchLimit = 32;

    internal const string SamplesSinceSql = """
        SELECT sample_time, cpu_percent, acu_utilization_percent, serverless_capacity_acu, max_configured_acu
        FROM pg_cpu_utilization
        WHERE server_id = $1
        AND   sample_time > $2
        AND   cpu_percent IS NOT NULL
        ORDER BY sample_time
        LIMIT $3
        """;

    /// <summary>
    /// Every reading newer than <paramref name="afterUtc"/>, OLDEST FIRST, for the High CPU persistence
    /// gate (#3282).
    ///
    /// <para><b>Why the gate cannot just re-read the latest row here, the way the SQL Server side does.</b>
    /// On SQL Server the alert sweep (30 s) is faster than the sample (about 60 s), so every sample is seen
    /// by at least one sweep and "latest row, counted once" loses nothing. Performance Insights is the
    /// opposite shape: <c>pg_cpu_utilization</c> is a five-minute collector ingesting 60-second data points,
    /// so five samples land at once and four of every five would never be counted. Requiring three
    /// consecutive breaching samples would then take three BATCHES — about fifteen minutes — which is a
    /// saturation event reported long after it mattered. Reading the batch makes three samples mean three
    /// minutes on both engines, which is also what lets one threshold keep meaning one thing across them.</para>
    ///
    /// <para><paramref name="afterUtc"/> is floored at <c>nowUtc - <see cref="Freshness"/></c>: a subject
    /// with no memory, or one whose collector was away for an hour, considers only readings recent enough to
    /// describe "right now" — the same bound <see cref="GetLatestAsync"/> applies, so the gate and the
    /// single-reading path agree on what counts as current.</para>
    /// </summary>
    public static async Task<System.Collections.Generic.List<CpuSample>> GetSamplesSinceAsync(
        NpgsqlDataSource postgres, int serverId, DateTime? afterUtc, DateTime nowUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var floor = nowUtc - Freshness;
        var after = afterUtc.HasValue && afterUtc.Value > floor ? afterUtc.Value : floor;

        var samples = new System.Collections.Generic.List<CpuSample>();
        await using var command = postgres.CreateCommand(SamplesSinceSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* Naive UTC at the bind, like every other comparison against the naive `timestamp` columns. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(after, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(GateBatchLimit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            samples.Add(new CpuSample(
                DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                reader.GetDouble(1),
                reader.IsDBNull(2) ? null : reader.GetDouble(2),
                reader.IsDBNull(3) ? null : reader.GetDouble(3),
                reader.IsDBNull(4) ? null : reader.GetDouble(4),
                /* The gate does not read memory: null here means "this read does not carry it", which is
                   a different statement from a row whose memory columns were NULL (see HostMemory). */
                Memory: null));
        }

        return samples;
    }

    /// <summary>
    /// The served read carries the V136 host-memory columns beside the CPU row (#3809, the third between-waves
    /// batch of #3691). Until then the six columns had exactly one reader — the memory family's collector in
    /// <c>PgTargetFactCollector.Memory.cs</c> — and an operator sent to <c>get_pg_cpu_utilization</c> by the
    /// memory facts' tool rows found no memory on it. The two ALERT reads (<see cref="LatestCpuSql"/>,
    /// <see cref="SamplesSinceSql"/>) still select what they selected before V136: the High CPU gate is a
    /// sub-second read on the alert path and has no memory question to answer.
    /// </summary>
    internal const string HistorySql = """
        SELECT sample_time, cpu_percent, acu_utilization_percent, serverless_capacity_acu, max_configured_acu,
               memory_total_bytes, memory_free_bytes, memory_cached_bytes, memory_buffers_bytes, memory_active_bytes,
               configured_memory_bytes
        FROM pg_cpu_utilization
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   cpu_percent IS NOT NULL
        ORDER BY sample_time
        """;

    /// <summary>
    /// One row's V136 host-memory columns, every member nullable: a pre-V136 row, or a Performance Insights
    /// endpoint without <c>os.memory.*</c>, has them NULL, and NULL travels as "not measured" rather than 0
    /// (a zero here would read as a host with no memory). Bytes, as stored; the collector's <c>HostMemorySample</c>
    /// is the same six in the analysis project, which cannot see this type.
    /// </summary>
    public sealed record HostMemory(
        long? TotalBytes,
        long? FreeBytes,
        long? CachedBytes,
        long? BuffersBytes,
        long? ActiveBytes,
        long? ConfiguredBytes);

    /// <param name="CpuPercent">Percent of the capacity CURRENTLY ALLOCATED (#3281).</param>
    /// <param name="AcuUtilizationPercent">Percent of the CONFIGURED ceiling in use, or null where
    /// Performance Insights had no capacity sample for this minute.</param>
    /// <param name="ServerlessCapacityAcu">ACU allocated at this minute, or null.</param>
    /// <param name="MaxConfiguredAcu">The configured ACU ceiling at this minute, or null.</param>
    /// <param name="Memory">The row's V136 host-memory columns on the SERVED read (<see cref="GetHistoryAsync"/>),
    /// and null on the alert-gate read (<see cref="GetSamplesSinceAsync"/>), whose SQL does not select them.
    /// Deliberately not defaulted, for the same reason the capacity trio is not: a null must mean "this read
    /// does not carry memory" by construction, never "someone forgot", and a row that WAS read with memory
    /// columns all NULL is a non-null record with null members — the two states stay distinguishable.</param>
    public sealed record CpuSample(
        DateTime SampleTimeUtc,
        double CpuPercent,
        double? AcuUtilizationPercent,
        double? ServerlessCapacityAcu,
        double? MaxConfiguredAcu,
        HostMemory? Memory);

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
                reader.IsDBNull(4) ? null : reader.GetDouble(4),
                new HostMemory(
                    TotalBytes: reader.IsDBNull(5) ? null : reader.GetInt64(5),
                    FreeBytes: reader.IsDBNull(6) ? null : reader.GetInt64(6),
                    CachedBytes: reader.IsDBNull(7) ? null : reader.GetInt64(7),
                    BuffersBytes: reader.IsDBNull(8) ? null : reader.GetInt64(8),
                    ActiveBytes: reader.IsDBNull(9) ? null : reader.GetInt64(9),
                    ConfiguredBytes: reader.IsDBNull(10) ? null : reader.GetInt64(10))));
        }

        return samples;
    }

    /// <summary>One bucketed point (#4193 - the TrendBuckets contract #3897 gave the rest of the trend family).
    /// CPU and ACU are averaged with each bucket's peak kept beside it, so a saturation minute survives a wide
    /// bucket; the capacity trio stays averaged, on <c>Rounded</c>'s terms; the memory pressure pair
    /// (<see cref="Memory"/>'s <c>FreeBytes</c>/<c>ActiveBytes</c>) is the bucket's WORST sample - minimum free,
    /// maximum active - rather than an average, so a brief pressure spike is not smoothed away; the other four
    /// memory columns stay averaged.</summary>
    public sealed record CpuBucketPoint(
        DateTime BucketStartUtc,
        double CpuPercent,
        double? PeakCpuPercent,
        double? AcuUtilizationPercent,
        double? PeakAcuUtilizationPercent,
        double? ServerlessCapacityAcu,
        double? MaxConfiguredAcu,
        long Samples,
        long CapacitySamples,
        HostMemory? Memory,
        long MemorySamples);

    /// <summary>date_bin buckets on <c>sample_time</c>, windowed on <c>collection_time</c> - the same split
    /// <see cref="HistorySql"/> takes (see its own doc comment) - and NOT clamped to the window's start, on the
    /// same terms as the SQL Server CPU trend's own bucketed query
    /// (<c>DarlingDataReader.CpuUtilizationBucketedSql</c>): a collection can carry a sample from before the
    /// window, and clamping would move it off the minute it actually landed on. $1 server_id, $2/$3 window
    /// (naive UTC), $4 bucket width in minutes.</summary>
    internal static readonly string HistoryBucketedSql = $"""
        SELECT
            date_bin(CAST($4 AS integer) * INTERVAL '1 minute', sample_time, {TrendBucketSql.OriginSql}) AS bucket_start,
            AVG(cpu_percent) AS cpu_percent,
            MAX(cpu_percent) AS peak_cpu_percent,
            AVG(acu_utilization_percent) AS acu_utilization_percent,
            MAX(acu_utilization_percent) AS peak_acu_utilization_percent,
            AVG(serverless_capacity_acu) AS serverless_capacity_acu,
            AVG(max_configured_acu) AS max_configured_acu,
            COUNT(*) AS samples,
            COUNT(acu_utilization_percent) AS capacity_samples,
            AVG(memory_total_bytes)::double precision AS memory_total_bytes,
            MIN(memory_free_bytes) AS memory_free_bytes,
            AVG(memory_cached_bytes)::double precision AS memory_cached_bytes,
            AVG(memory_buffers_bytes)::double precision AS memory_buffers_bytes,
            MAX(memory_active_bytes) AS memory_active_bytes,
            AVG(configured_memory_bytes)::double precision AS configured_memory_bytes,
            COUNT(memory_total_bytes) AS memory_samples
        FROM pg_cpu_utilization
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   cpu_percent IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """;

    /// <summary>Runs <see cref="HistoryBucketedSql"/> - the served read behind <c>get_pg_cpu_utilization</c>.</summary>
    public static async Task<System.Collections.Generic.List<CpuBucketPoint>> GetBucketedHistoryAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int bucketMinutes, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var points = new System.Collections.Generic.List<CpuBucketPoint>();
        await using var command = postgres.CreateCommand(HistoryBucketedSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(bucketMinutes);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            points.Add(new CpuBucketPoint(
                DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                reader.GetDouble(1),
                reader.IsDBNull(2) ? null : reader.GetDouble(2),
                reader.IsDBNull(3) ? null : reader.GetDouble(3),
                reader.IsDBNull(4) ? null : reader.GetDouble(4),
                reader.IsDBNull(5) ? null : reader.GetDouble(5),
                reader.IsDBNull(6) ? null : reader.GetDouble(6),
                reader.GetInt64(7),
                reader.GetInt64(8),
                new HostMemory(
                    TotalBytes: reader.IsDBNull(9) ? null : (long)Math.Round(reader.GetDouble(9)),
                    FreeBytes: reader.IsDBNull(10) ? null : reader.GetInt64(10),
                    CachedBytes: reader.IsDBNull(11) ? null : (long)Math.Round(reader.GetDouble(11)),
                    BuffersBytes: reader.IsDBNull(12) ? null : (long)Math.Round(reader.GetDouble(12)),
                    ActiveBytes: reader.IsDBNull(13) ? null : reader.GetInt64(13),
                    ConfiguredBytes: reader.IsDBNull(14) ? null : (long)Math.Round(reader.GetDouble(14))),
                reader.GetInt64(15)));
        }

        return points;
    }
}
