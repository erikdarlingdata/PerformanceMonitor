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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #7242. The exact runs and the gaps between them after a collector's newest streak break, for every Darling surface that bands
/// the activity quiet bar: the service's fleet card, the Viewer's Overview and both per-server reads (<c>get_collection_health</c>
/// and the Viewer's Collection Health tab). Every run after the newest run that was not a zero-row SUCCESS is itself a zero-row
/// SUCCESS, so these are the runs of the trailing zero-row streak.
///
/// <para>Why it exists: the quiet bar is measured in time the service observed, not wall time. The roll-ups used to estimate the run
/// count as elapsed time over the cadence (<c>CollectorHealthClassifier.EstimateTrailingZeroRowSuccessRuns</c>), which hands the wall
/// clock back, and runs times the interval reads low for a collector whose runs were spaced out. Observed time is the sum of each gap
/// between consecutive runs of the streak (from the run that broke it to the newest run), a gap counted at most
/// <see cref="ObservedQuietGaps.GapCapMinutes"/>, so a stretch the service was stopped adds only the cap. Asked only for the
/// activity-sourced rows whose last productive run is older than the bar, one keyed range read per pair, so a healthy fleet runs
/// nothing.</para>
/// </summary>
public static class CollectorObservedQuietRuns
{
    /// <summary>
    /// One row per requested (server, collector) pair: the run count after the streak break, the sum of the gaps of
    /// <see cref="ObservedQuietGaps.GapFloorMinutes"/> minutes or less, and each longer gap. $1 server ids, $2 collector names and $3
    /// the streak break times (parallel arrays, a NULL break is found here the way every health read defines it), $4 the window start
    /// (naive UTC, the roll-up's own), $5 the gap floor in minutes. One LATERAL per pair reads the runs strictly after the break
    /// through the (server, collector, collection_time) index. The first run's gap starts at the break itself.
    /// </summary>
    public static readonly string FleetRunsSql = $@"
SELECT p.server_id,
       p.collector_name,
       r.runs,
       r.short_minutes,
       r.long_gaps
FROM unnest($1::integer[], $2::text[], $3::timestamp[]) AS p(server_id, collector_name, break_time)
CROSS JOIN LATERAL
(
    SELECT COALESCE(p.break_time,
                    (SELECT MAX(b.collection_time)
                     FROM v_collection_log b
                     WHERE b.server_id = p.server_id
                     AND   b.collector_name = p.collector_name
                     AND   b.collection_time >= $4
                     AND   NOT (b.status = 'SUCCESS'
                                AND COALESCE(b.rows_collected, 0) = 0
                                AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql.Replace("rows_collected", "b.rows_collected").Replace("error_message", "b.error_message")}))) AS break_time
) bt
CROSS JOIN LATERAL
(
    SELECT COUNT(*) AS runs,
           COALESCE(SUM(g.gap_minutes) FILTER (WHERE g.gap_minutes <= $5::double precision), 0) AS short_minutes,
           COALESCE(array_agg(g.gap_minutes) FILTER (WHERE g.gap_minutes > $5::double precision), ARRAY[]::double precision[]) AS long_gaps
    FROM
    (
        SELECT EXTRACT(EPOCH FROM (v.collection_time - COALESCE(LAG(v.collection_time) OVER w, bt.break_time)))::double precision / 60.0 AS gap_minutes
        FROM v_collection_log v
        WHERE v.server_id = p.server_id
        AND   v.collector_name = p.collector_name
        AND   v.collection_time >= $4
        AND   v.collection_time > bt.break_time
        WINDOW w AS (ORDER BY v.collection_time)
    ) g
) r";

    /// <summary>
    /// Reads the runs and gaps for each request. Returns an empty map without touching the store when <paramref name="requests"/> is empty.
    /// A null break time is looked up in the statement.
    /// </summary>
    public static async Task<Dictionary<(int ServerId, string CollectorName), ObservedQuietGaps>> ReadAsync(
        NpgsqlDataSource dataSource,
        IReadOnlyList<(int ServerId, string CollectorName, DateTime? BreakTime)> requests,
        DateTime windowStart,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        var results = new Dictionary<(int ServerId, string CollectorName), ObservedQuietGaps>();
        if (requests.Count == 0)
        {
            return results;
        }

        var serverIds = new int[requests.Count];
        var collectors = new string[requests.Count];
        var breaks = new DateTime?[requests.Count];
        for (var i = 0; i < requests.Count; i++)
        {
            serverIds[i] = requests[i].ServerId;
            collectors[i] = requests[i].CollectorName;
            breaks[i] = requests[i].BreakTime is { } breakTime
                ? DateTime.SpecifyKind(breakTime, DateTimeKind.Unspecified)
                : null;
        }

        await using var command = dataSource.CreateCommand(FleetRunsSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = serverIds });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = collectors });
        command.Parameters.Add(new NpgsqlParameter<DateTime?[]> { TypedValue = breaks });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified)
        });
        command.Parameters.Add(new NpgsqlParameter<double> { TypedValue = ObservedQuietGaps.GapFloorMinutes });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results[(reader.GetInt32(0), reader.GetString(1))] = new ObservedQuietGaps(
                reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3)),
                reader.IsDBNull(4) ? Array.Empty<double>() : reader.GetFieldValue<double[]>(4));
        }

        return results;
    }
}
