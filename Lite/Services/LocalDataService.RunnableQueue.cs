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
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// The Runnable Queue alert's sample read (#5641): one <see cref="RunnableQueueSample"/> per
    /// <c>cpu_scheduler_stats</c> <c>collection_time</c> in the last <paramref name="lookbackMinutes"/> minutes,
    /// newest first. A collection with one row uses that row's <c>avg_runnable_tasks_count</c> exactly; a collection the
    /// collector split across several rows (each covering a subset of the schedulers while saying the full
    /// <c>scheduler_count</c>, #5710) uses <c>SUM(total_runnable_tasks_count) / MAX(scheduler_count)</c>, folded by the shared
    /// <see cref="CpuSchedulerCollectionSql"/>. The twin of
    /// <c>DarlingAlertReadAdapter.RunnableQueueSql</c>, in DuckDB's dialect. See <see cref="RunnableQueueSamplesSql"/>.
    /// </summary>
    public async Task<IReadOnlyList<RunnableQueueSample>> GetRunnableQueueSamplesAsync(int serverId, int lookbackMinutes)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = RunnableQueueSamplesSql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        /* The caller's lookback, as a naive-UTC floor: DuckDB keeps microseconds in collection_time, and this is the
           clock style the poison-wait read binds. */
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddMinutes(-lookbackMinutes) });

        var samples = new List<RunnableQueueSample>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            /* A collection whose scheduler count is NULL on a split collection has no per-scheduler value: not a sample. */
            if (reader.IsDBNull(1))
            {
                continue;
            }

            samples.Add(new RunnableQueueSample(
                reader.GetDateTime(0),
                reader.GetDouble(1),
                reader.IsDBNull(2) ? null : reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3)));
        }

        return samples;
    }

    /// <summary>
    /// <see cref="GetRunnableQueueSamplesAsync"/>'s text, exposed for the parity pins. One row per
    /// <c>collection_time</c>, newest first (no tie is possible after the GROUP BY). The per-collection fold is NOT
    /// written here: it is <see cref="CpuSchedulerCollectionSql.PerCollection"/> (#5710; one definition for every reader,
    /// #5641 review), and this select takes <c>avg_runnable_tasks_count</c> from it as <c>per_scheduler</c>: the lone
    /// row's own average, or the summed total over the highest scheduler count when the collection was split. A row whose
    /// <c>avg_runnable_tasks_count</c> is NULL (no visible online scheduler) is not a row: the filter is on the stored
    /// rows, before the fold. $1 server_id, $2 the naive-UTC floor.
    /// </summary>
    public static readonly string RunnableQueueSamplesSql = $"""
SELECT
    collection_time,
    CAST(avg_runnable_tasks_count AS DOUBLE) AS per_scheduler,
    CAST(total_runnable_tasks_count AS BIGINT) AS total_runnable_tasks_count,
    CAST(scheduler_count AS INTEGER) AS scheduler_count
FROM
(
    {CpuSchedulerCollectionSql.PerCollection("v_cpu_scheduler_stats", "server_id = $1 AND collection_time >= $2 AND avg_runnable_tasks_count IS NOT NULL", newestOnly: false)}
) AS collections
ORDER BY collection_time DESC
""";
}
