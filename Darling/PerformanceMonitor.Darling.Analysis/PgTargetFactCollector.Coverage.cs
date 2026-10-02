/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// The PostgreSQL-target coverage witness (#3538 A2, #3542): how much of the window the collector actually
    /// observed, from the <c>pg_database_stats</c> collection series. <see cref="PgFactCollector.CoverageSql"/>
    /// with ONE substitution — the table — and the same five-column shape, so both engines finish through the
    /// same <see cref="PgFactCollector.BuildCoverage"/>. The rules the parameters make concrete are that
    /// query's, argued there and in <see cref="WindowCoverage"/>: <c>$1..$3</c> server and window;
    /// <c>$4</c> = window start minus one gap policy, so the first in-window row can find its predecessor;
    /// <c>$5</c> = <see cref="CollectorDeltaCalculator.DefaultMaxGapSeconds"/>, bound rather than inlined so the
    /// witness and the calculator that produced the deltas cannot disagree about where "observed" ends.
    ///
    /// <para><b>Why this table (D3).</b> The SQL Server witness is <c>v_wait_stats</c> because that is the one
    /// series every SQL Server target writes every minute. Its PostgreSQL equivalent is <c>pg_database_stats</c>:
    /// one-minute cadence, every flavour (Aurora writer and reader, RDS, self-hosted, replica), no extension
    /// required — where <c>pg_wait_stats</c> is Aurora-only and <c>pg_wait_sampling</c> is hourly and needs an
    /// extension. The same table answers the 24-hour data-span gate (<see cref="DarlingAnalysisService.PgTargetDataSpanSql"/>),
    /// mirroring how <c>wait_stats</c> answers both gates on the SQL Server side.</para>
    ///
    /// <para><c>DISTINCT collection_time</c> because a collection writes one row PER DATABASE; the interval
    /// belongs to the collection, not to each of its rows.</para>
    /// </summary>
    public const string PgTargetCoverageSql = @"
WITH samples AS (
    SELECT DISTINCT collection_time
    FROM pg_database_stats
    WHERE server_id = $1
    AND   collection_time >= $4
    AND   collection_time <= $3
),
intervals AS (
    SELECT collection_time,
           LAG(collection_time) OVER (ORDER BY collection_time) AS previous_time
    FROM samples
)
SELECT
    COUNT(*) AS sample_count,
    COALESCE(SUM(CASE
        WHEN previous_time IS NULL THEN 0
        WHEN EXTRACT(EPOCH FROM (collection_time - previous_time)) > $5 THEN 0
        ELSE EXTRACT(EPOCH FROM (collection_time - GREATEST(previous_time, $2)))
    END), 0) AS observed_seconds,
    COALESCE(MAX(CASE
        WHEN previous_time IS NOT NULL AND EXTRACT(EPOCH FROM (collection_time - previous_time)) > $5
        THEN EXTRACT(EPOCH FROM (collection_time - GREATEST(previous_time, $2)))
        ELSE 0
    END), 0) AS largest_discarded_seconds,
    COALESCE(SUM(CASE WHEN previous_time IS NULL THEN 1 ELSE 0 END), 0) AS orphan_count,
    MIN(collection_time) AS first_sample,
    MAX(collection_time) AS last_sample
FROM intervals
WHERE collection_time >= $2";

    /// <summary>
    /// Step 0 of every PostgreSQL-target pass: stamps <see cref="AnalysisContext.Coverage"/> from
    /// <see cref="PgTargetCoverageSql"/> and, when the window was only partly observed, emits the
    /// <see cref="WindowCoverage.FactKey"/> context fact beside the facts it qualifies.
    ///
    /// <para>This is the seam #3605 and the design doc could not name, and without it the whole engine is
    /// inert for PostgreSQL: the service's empty-window branch reads <c>ObservedDurationMs &lt;= 0</c> as "the
    /// collector observed none of the window" and returns <c>unavailable</c>, so a collector that never stamps
    /// coverage makes EVERY pass report a dead collector — on a target whose collectors are running fine.</para>
    ///
    /// <para>Like its SQL Server twin it carries no catch: this is the canary series, and a store that cannot
    /// answer it should fail the pass loudly (the service's catch classifies and logs it) rather than degrade
    /// into "unobserved", which would report a dead collector for a store that merely timed out. A zero-length
    /// or reversed window skips the read and stamps unobserved.</para>
    /// </summary>
    private async partial Task CollectObservedCoverageAsync(AnalysisContext context, List<Fact> facts)
    {
        var nominalMs = context.PeriodDurationMs;
        if (nominalMs <= 0)
        {
            context.Coverage = WindowCoverage.Unobserved(nominalMs);
            return;
        }

        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var command = new NpgsqlCommand(PgTargetCoverageSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        command.Parameters.AddWithValue(context.ServerId);
        command.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        command.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        command.Parameters.AddWithValue(AsNaive(context.TimeRangeStart.AddSeconds(-CollectorDeltaCalculator.DefaultMaxGapSeconds)));
        command.Parameters.AddWithValue(CollectorDeltaCalculator.DefaultMaxGapSeconds);

        using var reader = await command.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken))
        {
            context.Coverage = WindowCoverage.Unobserved(nominalMs);
            return;
        }

        var sampleCount = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
        var observedSeconds = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
        var largestDiscardedSeconds = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
        var orphanCount = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
        DateTime? firstSample = reader.IsDBNull(4) ? null : reader.GetDateTime(4);
        DateTime? lastSample = reader.IsDBNull(5) ? null : reader.GetDateTime(5);

        context.Coverage = PgFactCollector.BuildCoverage(
            context, nominalMs, sampleCount, observedSeconds, largestDiscardedSeconds, orphanCount, firstSample, lastSample);

        if (context.Coverage.IsPartial)
            facts.Add(context.Coverage.ToGapFact(context.ServerId));
    }
}
