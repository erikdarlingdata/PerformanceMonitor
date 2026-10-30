/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>The newest <see cref="CollectorRunFacts.RunsCap"/> successful runs of one collector, newest first. $1 server_id, $2 collector_name, $3 the cap (bound, never a literal).</summary>
    public const string CollectorLatestSuccessSql = @"
SELECT collection_time, rows_collected
FROM v_collection_log
WHERE server_id = $1
AND   collector_name = $2
AND   status = 'SUCCESS'
ORDER BY collection_time DESC
LIMIT $3";

    /// <summary><see cref="CollectorLatestSuccessSql"/> for a historical read (#5633 review): only runs AT OR BEFORE <c>$4</c>, the read's
    /// as_of, so an empty page answers with the collector's state as of the anchor, not as of now. Twin of
    /// <c>DarlingCollectorRunFactsReader.LatestSuccessAtOrBeforeSql</c>.</summary>
    public const string CollectorLatestSuccessAtOrBeforeSql = @"
SELECT collection_time, rows_collected
FROM v_collection_log
WHERE server_id = $1
AND   collector_name = $2
AND   status = 'SUCCESS'
AND   collection_time <= $4
ORDER BY collection_time DESC
LIMIT $3";

    /// <summary>
    /// The successful trace_flags runs that stored ZERO rows (#5633 item 19), the empty snapshots the trace-flag change diff needs: a run
    /// that finds no flag stores nothing, so without these the last flag being turned off never shows. Bounded like the change read: the
    /// runs inside the window plus the newest one before it (the baseline for a change on the window's first capture). SUCCESS only, and
    /// <c>rows_collected = 0</c> only (NULL is not recorded). Twin of <c>DarlingRetentionHorizons.EmptyTraceFlagRunsSql</c>.
    /// The baseline run is bounded below at $4, <see cref="EmptyTraceFlagRunsFloor"/>: an empty run older than the oldest trace_flags rows
    /// Lite still holds would pair with a later capture as a flag turned on, and the bound also lets DuckDB prune the collection log's
    /// archive instead of scanning all of it.
    /// $1 server_id, $2 window start, $3 window end, $4 the baseline floor.
    /// </summary>
    public const string EmptyTraceFlagRunsSql = @"
(SELECT collection_time
 FROM v_collection_log
 WHERE server_id = $1
 AND   collector_name = 'trace_flags'
 AND   status = 'SUCCESS'
 AND   rows_collected = 0
 AND   collection_time >= $2
 AND   collection_time <= $3)
UNION ALL
(SELECT collection_time
 FROM v_collection_log
 WHERE server_id = $1
 AND   collector_name = 'trace_flags'
 AND   status = 'SUCCESS'
 AND   rows_collected = 0
 AND   collection_time < $2
 AND   collection_time >= $4
 ORDER BY collection_time DESC
 LIMIT 1)";

    /// <summary>
    /// The lower bound of the empty-run baseline (#5633 item 19): the window start minus Lite's retention. Lite has ONE horizon for every
    /// table, <see cref="RetentionService.ArchiveRetentionMonths"/> (its per-collector <c>retention_days</c> does not purge the archive),
    /// so that is the retention that applies to the trace_flags rows too.
    /// </summary>
    internal static DateTime EmptyTraceFlagRunsFloor(DateTime windowStartUtc) =>
        windowStartUtc.AddMonths(-RetentionService.ArchiveRetentionMonths);

    /// <summary>The times of the trace_flags runs that found no flag (<see cref="EmptyTraceFlagRunsSql"/>), for the trace-flag change diff.</summary>
    public async Task<List<DateTime>> GetEmptyTraceFlagRunsAsync(int serverId, DateTime startUtc, DateTime endUtc)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        return await ReadEmptyTraceFlagRunsAsync(command, serverId, startUtc, endUtc, CancellationToken.None);
    }

    /// <summary>Runs <see cref="EmptyTraceFlagRunsSql"/> on a command from an open connection; shared with the analysis service's own connection.</summary>
    internal static async Task<List<DateTime>> ReadEmptyTraceFlagRunsAsync(
        DuckDBCommand command, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var times = new List<DateTime>();
        command.CommandText = EmptyTraceFlagRunsSql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startUtc });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });
        command.Parameters.Add(new DuckDBParameter { Value = EmptyTraceFlagRunsFloor(startUtc) });
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            times.Add(reader.GetDateTime(0));
        }

        return times;
    }

    /// <summary>Successful runs of one collector inside a window. $1 server_id, $2 collector_name, $3/$4 window.</summary>
    public const string CollectorRunsInWindowSql = @"
SELECT COUNT(*)
FROM v_collection_log
WHERE server_id = $1
AND   collector_name = $2
AND   status = 'SUCCESS'
AND   collection_time >= $3
AND   collection_time <= $4";

    /// <summary>
    /// #5633 items 3 and 19: what <c>collection_log</c> says about one collector (the Darling twin is
    /// <c>DarlingCollectorRunFactsReader</c>; the words are shared in <see cref="CollectorRunFacts"/>). Pass
    /// <paramref name="hoursBack"/> (with <paramref name="asOfUtc"/>) to also count the runs inside the window; leave it
    /// null for a read with no window question. A non-null <paramref name="asOfUtc"/> also bounds the newest-run read at the anchor
    /// (<see cref="CollectorLatestSuccessAtOrBeforeSql"/>), with or without a window. Only SUCCESS rows count. Asked only on an empty path, and a failed read
    /// propagates to the tool's own catch rather than becoming a quiet "no runs". <paramref name="window"/> (#6651) counts the runs inside
    /// that UTC window instead of the last <paramref name="hoursBack"/> hours.
    /// </summary>
    public async Task<CollectorRunFacts> GetCollectorRunFactsAsync(int serverId, string collectorName, int? hoursBack = null, DateTime? asOfUtc = null,
        (DateTime StartUtc, DateTime EndUtc)? window = null)
    {
        using var connection = await OpenConnectionAsync();

        long runs = 0;
        DateTime? last = null;
        long? lastRows = null;

        using (var command = connection.CreateCommand())
        {
            command.CommandText = asOfUtc is null ? CollectorLatestSuccessSql : CollectorLatestSuccessAtOrBeforeSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = collectorName });
            command.Parameters.Add(new DuckDBParameter { Value = CollectorRunFacts.RunsCap });
            if (asOfUtc is { } ceiling)
            {
                command.Parameters.Add(new DuckDBParameter { Value = ceiling });
            }

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                runs++;
                if (runs == 1)
                {
                    last = reader.IsDBNull(0) ? null : DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc);
                    lastRows = reader.IsDBNull(1) ? null : Convert.ToInt64(reader.GetValue(1));
                }
            }
        }

        long? inWindow = null;
        if (window is not null || hoursBack is not null)
        {
            /* #6651: a grid's custom range or slicer window counts the runs inside that exact window (GetQueriesTabWindowUtc), not the
               last N hours. */
            var (startTime, endTime) = window is { } given ? (given.StartUtc, given.EndUtc) : GetTimeRange(hoursBack!.Value, null, null, asOfUtc);
            using var command = connection.CreateCommand();
            command.CommandText = CollectorRunsInWindowSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = collectorName });
            command.Parameters.Add(new DuckDBParameter { Value = startTime });
            command.Parameters.Add(new DuckDBParameter { Value = endTime });
            inWindow = Convert.ToInt64(await command.ExecuteScalarAsync() ?? 0L);
        }

        return new CollectorRunFacts(runs, inWindow, last, lastRows);
    }
}
