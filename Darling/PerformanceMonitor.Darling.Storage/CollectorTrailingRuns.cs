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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #5646, #5666. The newest-runs success streak a collector's band reads
/// (<c>CollectorHealthClassifier.Classify</c>'s <c>trailingSuccessRuns</c>), as ONE definition for every read
/// of it: the per-server Collection Health statements (the service's and the viewer's) interpolate
/// <see cref="RunOkSql"/> and <see cref="StreakFromFlagsSql"/>, and the fleet roll-ups' follow-up statement
/// (<see cref="FleetStreakSql"/>) is built from the same two pieces, so a recovered collector bands the same on
/// a server's tab and on the fleet card and the two cannot drift.
///
/// <para>The fleet roll-ups themselves stay plain aggregates (no fleet-wide sort of a week of
/// <c>collection_log</c> in front of their GROUP BY, #3735). This read is asked only for the
/// (server, collector) pairs whose band the streak could change, so a healthy fleet runs nothing.</para>
/// </summary>
public static class CollectorTrailingRuns
{
    /// <summary>1 when the run (alias <c>v</c> over <c>v_collection_log</c>) succeeded, else 0. An abandonment that
    /// predates #2803 is stored as SUCCESS and is not a success here (the shared #2926 predicate).</summary>
    public const string RunOkSql = $@"CASE WHEN v.status = 'SUCCESS'
                                 AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql}
                                THEN 1 ELSE 0 END";

    /// <summary>The leading-success count (0 to 3) from the newest-first array <c>r.flags</c> of
    /// <see cref="RunOkSql"/> values. Counted to
    /// <c>CollectorHealthClassifier.RecoveryTrailingSuccessRuns</c> (3) and no further.</summary>
    public const string StreakFromFlagsSql = @"CASE WHEN r.flags[1] = 1 AND r.flags[2] = 1 AND r.flags[3] = 1 THEN 3
                        WHEN r.flags[1] = 1 AND r.flags[2] = 1 THEN 2
                        WHEN r.flags[1] = 1 THEN 1
                        ELSE 0 END";

    /// <summary>
    /// The fleet follow-up: one row per requested (server, collector) pair with its streak. $1 server ids and
    /// $2 collector names (parallel arrays), $3 the window start (naive UTC, the roll-up's own). One LATERAL
    /// per pair reads the newest three runs of the window, <c>collection_time DESC, log_id DESC</c>, with a
    /// LIMIT 3 - a keyed index walk that stops after three rows, no sort of the window.
    /// </summary>
    public const string FleetStreakSql = $@"
SELECT p.server_id,
       p.collector_name,
       {StreakFromFlagsSql} AS trailing_success_runs
FROM unnest($1::integer[], $2::text[]) AS p(server_id, collector_name)
CROSS JOIN LATERAL
(
    SELECT array_agg(x.ok ORDER BY x.collection_time DESC, x.log_id DESC) AS flags
    FROM
    (
        SELECT {RunOkSql} AS ok,
               v.collection_time,
               v.log_id
        FROM v_collection_log v
        WHERE v.server_id = p.server_id
        AND   v.collector_name = p.collector_name
        AND   v.collection_time >= $3
        ORDER BY v.collection_time DESC, v.log_id DESC
        LIMIT 3
    ) x
) r";

    /// <summary>
    /// Reads the streak for each pair. Returns an empty map without touching the store when
    /// <paramref name="pairs"/> is empty. A pair with no runs in the window is absent from the map (read as 0).
    /// </summary>
    public static async Task<Dictionary<(int ServerId, string CollectorName), long>> ReadAsync(
        NpgsqlDataSource dataSource,
        IReadOnlyList<(int ServerId, string CollectorName)> pairs,
        DateTime windowStart,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        var streaks = new Dictionary<(int ServerId, string CollectorName), long>();
        if (pairs.Count == 0)
        {
            return streaks;
        }

        var serverIds = new int[pairs.Count];
        var collectors = new string[pairs.Count];
        for (var i = 0; i < pairs.Count; i++)
        {
            serverIds[i] = pairs[i].ServerId;
            collectors[i] = pairs[i].CollectorName;
        }

        await using var command = dataSource.CreateCommand(FleetStreakSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = serverIds });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = collectors });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified)
        });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            streaks[(reader.GetInt32(0), reader.GetString(1))] =
                reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2));
        }

        return streaks;
    }
}
